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

| #   | Item                                                                                                                              | Status                                                                                           |
| --- | --------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| 1   | [PIP-460 scalable-topics e2e](#1-pip-460-scalable-topics-e2e)                                                                     | ⏳ scaffold in place; stub bodies trivially pass; flesh out once a Pulsar 5.0 RC carries PIP-460 |
| 2   | [Wrapper consumers/producers cannot drive `record_rate_window`](#2-wrapper-rate-window-fan-out)                                   | 🧠 needs design decision on the fan-out surface                                                  |
| 3   | [`Instant::elapsed()` clock leak in proto latency histograms](#3-latency-histogram-clock-leak)                                    | ⚡ ready to dispatch                                                                             |
| 8   | [Broker-URL authority parsers are not unified on `probe_authority`](#8-broker-url-authority-parser-unification)                   | 🟡 deferred (not load-bearing)                                                                   |
| 9   | [`e2e_transparent_inflight_publish_replay_across_broker_restart` races `send_timeout`](#9-inflight-replay-e2e-races-send_timeout) | ⚡ ready to dispatch                                                                             |

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

## 8. Broker-URL authority parser unification

**Gap.** [ADR-0085](../specs/adr/0085-probe-endpoint-parsing-in-proto.md) single-sourced the **health-probe** endpoint parse into `magnetar_proto::probe_authority`, but two sibling parsers with the same arm-for-arm shape still live in `crates/magnetar-runtime-moonpool/src/client.rs`: `proxy_broker_authority` and `direct_broker_authority`.
All three now agree on behaviour — reject an unrecognised `"://"` scheme, synthesise the scheme default port, pass a bare `host:port` through — but they agree by having been written to match, not by construction.
That is exactly the arrangement that produced the ADR-0085 defect in the first place: two copies of one rule, drifting silently.

Concretely, the shared limitation they must keep in lockstep is the port-less bracketed IPv6 case (`pulsar://[::1]` gets no synthesised port, because the synthesis triggers on "authority contains no `:`").
Fixing it in one place without the other is precisely the drift this entry exists to prevent.

**Full site inventory** (from the `strip_prefix("pulsar` sweep run while landing ADR-0085), so a unifier does not merge parsers that are deliberately different:

| Site                                                                                           | Contract                                                                              | Unify?                                                                                                                                                                                                                                                                                          |
| ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `magnetar_proto::probe_authority`                                                              | scheme optional; bare `host:port` accepted; default port synthesised                  | canonical — the target                                                                                                                                                                                                                                                                          |
| `magnetar-runtime-moonpool/src/client.rs` `proxy_broker_authority` / `direct_broker_authority` | same rules, `Result<_, ClientError>`                                                  | **yes** — this entry                                                                                                                                                                                                                                                                            |
| `magnetar-runtime-moonpool/src/driver.rs` `strip_url_to_host_port`                             | scheme **required** (a bare `host:port` returns `None`); also trims `?` / `#`         | **no** — stricter on purpose (service URLs must carry a scheme). Already correct: it uses `?` on the `strip_prefix` chain, never `unwrap_or`, so it has never had the ADR-0085 defect.                                                                                                          |
| `magnetar-proto/src/conn_types.rs` `extract_pulsar_host`                                       | returns the **host only**, no port; IPv6-bracket carve-out                            | **no** — different job (allow-list host matching, ADR-0044 redirect gate)                                                                                                                                                                                                                       |
| `magnetar-runtime-tokio/src/client.rs` `parse_direct_broker_url`                               | returns `ParsedUrl { host, port }`, not an authority string; `Result<_, ClientError>` | **maybe** — it grew its own `broker_url.contains("://")` guard when the tokio DIRECT-path gap was closed, so the reject rule now has a fifth independent copy. Worth folding in, but it parses to a struct rather than a `host:port` string, so it needs a different seam than the other three. |

**Why it stays open.** Not a behavioural bug — it is a duplication that is currently correct, so the payoff is drift prevention rather than a fix.
The two client-side parsers return `Result<String, ClientError>` with caller-specific error text and sit on the lookup/routing path, whose blast radius (a corrupted value reaching `CommandConnect.proxy_to_broker_url` on the wire, per issue #364) differs from a probe verdict.
Unifying them means either threading a proto-level error type into `ClientError` or having the callers map `None` to their own message, plus re-validating the routing path — wider than the probe hardening that surfaced it.

**`/goal`.**

```text
/goal unify the broker-URL authority parsers per docs/follow-ups.md §8: refactor proxy_broker_authority and direct_broker_authority in crates/magnetar-runtime-moonpool/src/client.rs to delegate their scheme/port parsing to magnetar_proto::probe_authority (added by ADR-0085) rather than each re-implementing the same arms, mapping None to the existing ClientError::Other messages so the caller-visible error text and the routing-path behaviour are unchanged. Prove equivalence with a table-driven test covering every input class both functions already pin (recognised schemes, unrecognised scheme, bare host:port, default-port synthesis, port-less bracketed IPv6), then close the shared port-less-IPv6 limitation in probe_authority so all three parsers gain the fix at once. Ship the ADR-0024 four-layer test set. Validation chain per CLAUDE.md.
```

---

## 9. Inflight-replay e2e races `send_timeout`

**Gap.** `e2e_transparent_inflight_publish_replay_across_broker_restart` (`crates/magnetar/tests/e2e_reconnect.rs`) is flaky on a loaded machine.
It `docker restart`s the broker with publishes in flight and asserts every `SendFut` resolves `Ok`, but it builds its client with only `operation_timeout(2 min)` — leaving `send_timeout` at its 30 s Java-parity default ([ADR-0072](../specs/adr/0072-java-parity-default-send-timeout.md), `crates/magnetar-proto/src/conn_types.rs`).
A `pulsar standalone` container restart is a full JVM boot; on a busy runner it exceeds 30 s, `send_timeout` fires on the relocated publishes, and the test panics at `e2e_reconnect.rs:485` with `SendRejected { code: -1, message: "send timeout" }`.

The failing runs log `send timed out while relocated across reconnect` immediately before the panic — the timeout is being enforced **correctly**; the test's own budget is simply shorter than the restart it triggers.

**Measured** (single test, isolated runs, 2026-07-29, 20-core workstation under concurrent build load):

| Tree                            | Runs | Failures |
| ------------------------------- | ---- | -------- |
| `main` (clean)                  | 6    | 1        |
| `main` + the ADR-0085 changeset | 4    | 2        |

Both sides flake, so this is **not** a regression from any one changeset — it was surfaced (not caused) by the #369 `send_timeout` hardening, which made publishes relocated across a reconnect visible to timeout enforcement for the first time; before that they were silently exempt, so a slow restart went unnoticed.

**Why it stays open.** The fix is a one-line test-side budget change (`.send_timeout(...)` above the worst-case container restart, or an explicit assertion that the restart completed inside the budget), but picking the number needs a measured worst case across the CI runner profile rather than a guess, and this is a test-harness timing bug rather than a client defect.
Per [ADR-0021](../specs/adr/0021-no-silent-test-ignore-or-remove.md) it must NOT be `#[ignore]`d or have its assertion loosened — the assertion is correct; only the budget is wrong.

**`/goal`.**

```text
/goal de-flake e2e_transparent_inflight_publish_replay_across_broker_restart per docs/follow-ups.md §9: the test leaves send_timeout at its 30s default while docker-restarting a pulsar standalone container whose JVM boot can exceed 30s on a loaded runner, so the relocated publishes correctly time out and the test panics at crates/magnetar/tests/e2e_reconnect.rs:485 with SendRejected { message: "send timeout" }. First measure the restart duration across several runs under load and record it in the test as a comment, then raise ONLY the client's send_timeout for this test above that measured worst case (do NOT weaken the Ok-resolution assertion, do NOT add #[ignore], and do NOT raise operation_timeout as a substitute). Prove the fix by running the single test 10 times under artificial CPU load with zero failures. Check whether the sibling broker-restart tests in the same file share the gap. Validation chain per CLAUDE.md.
```

---

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

§1 is a fully external blocker (the PIP-460 e2e flesh-out waits on a Pulsar 5.0 RC carrying PIP-460); §2 waits on a fan-out API design call; §8 is a drift-prevention refactor with no behavioural bug behind it; §3 and §9 are dispatch-ready.
Numbering is stable, not contiguous: closed items are removed and their number is retired rather than reused, so a `§N` reference in a commit, ADR, or code comment keeps pointing at the same item forever.
