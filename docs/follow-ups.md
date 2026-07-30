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

| #   | Item                                                                                                       | Status                                                                                           |
| --- | ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| 1   | [PIP-460 scalable-topics e2e](#1-pip-460-scalable-topics-e2e)                                              | ⏳ scaffold in place; stub bodies trivially pass; flesh out once a Pulsar 5.0 RC carries PIP-460 |
| 2   | [Wrapper consumers/producers cannot drive `record_rate_window`](#2-wrapper-rate-window-fan-out)            | 🧠 needs design decision on the fan-out surface                                                  |
| 8   | [Broker-URL authority parser unification — residual](#8-broker-url-authority-parser-unification--residual) | 🟡 deferred (three of four parsers unified; `parse_direct_broker_url` audited, not unified)      |

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

## 8. Broker-URL authority parser unification — residual

**Closed.** The three parsers that re-implemented `magnetar_proto::probe_authority`'s rule arm-for-arm now delegate to it: `proxy_broker_authority` / `direct_broker_authority` (`crates/magnetar-runtime-moonpool/src/client.rs`) and `strip_url_to_host_port` (`crates/magnetar-runtime-moonpool/src/driver.rs`, which gates its mandatory scheme and its `?` / `#` trim locally, then delegates, so it keeps its stricter contract without keeping a copy of the rule).
That also closed the shared port-less bracketed-IPv6 gap and an empty-authority hole in `proxy_broker_authority` that let `""` and `"pulsar://"` reach `CommandConnect.proxy_to_broker_url` as `""` and `":6650"`.
See [ADR-0087](../specs/adr/0087-unify-broker-url-authority-parsers.md) for the post-implementation reference.

**Remaining.** `magnetar_runtime_tokio::client::parse_direct_broker_url` is a fifth application of the same rule, and stays independent.
It parses via the `url` crate into `ParsedUrl { host, port }` rather than producing a `host:port` string, so folding it in would mean either giving up the struct return or wrapping `probe_authority` and re-splitting its output — trading a real seam for a cosmetic one.

It is **audited rather than unified**: `parse_direct_broker_url_agrees_with_probe_authority` is a table-driven test pinning, row by row, where the two agree and where they deliberately diverge (a scheme-less input takes the _bootstrap_ scheme's default port here but passes through port-less in `probe_authority`; a malformed bracket like `pulsar://[::1` is rejected by `url` but returned verbatim by `probe_authority`).
So a divergence can still be introduced, but not without editing a table that states the rationale.

One cosmetic residual inside it: its rejection message says an input "carries an unrecognised scheme" even for `"pulsar://"`, whose actual fault is the missing authority — the same imprecision ADR-0087 fixed on the moonpool side.
Left alone deliberately, since changing it is a user-visible text change with no correctness content.

**Why the residual stays open.** No behavioural bug and no drift that a test cannot see; closing it is an API-shape question (does the DIRECT path want an authority string or a parsed struct?) rather than a correctness one.

**Site inventory**, kept so a future unifier does not merge parsers that are deliberately different:

| Site                                                                                           | Contract                                                                              | Status                                                                                                                                            |
| ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `magnetar_proto::probe_authority`                                                              | scheme optional; bare `host:port` accepted; default port synthesised                  | canonical — the single implementation                                                                                                             |
| `magnetar-runtime-moonpool/src/client.rs` `proxy_broker_authority` / `direct_broker_authority` | same rules, `Result<_, ClientError>`                                                  | **unified** (ADR-0087) — delegates, maps `None` to one `ClientError::Other`                                                                       |
| `magnetar-runtime-moonpool/src/driver.rs` `strip_url_to_host_port`                             | scheme **required** (a bare `host:port` returns `None`); also trims `?` / `#`         | **unified** (ADR-0087) — gates its stricter contract locally, then delegates                                                                      |
| `magnetar-proto/src/conn_types.rs` `extract_pulsar_host`                                       | returns the **host only**, no port; IPv6-bracket carve-out                            | **no** — different job (allow-list host matching, ADR-0044 redirect gate)                                                                         |
| `magnetar-runtime-tokio/src/client.rs` `parse_direct_broker_url`                               | returns `ParsedUrl { host, port }`, not an authority string; `Result<_, ClientError>` | **audited, not unified** — this residual; equivalence pinned by `parse_direct_broker_url_agrees_with_probe_authority` rather than by construction |

---

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

§1 is a fully external blocker (the PIP-460 e2e flesh-out waits on a Pulsar 5.0 RC carrying PIP-460); §2 waits on a fan-out API design call; §8 is trimmed to one audited-not-unified parser whose closure is an API-shape question.
Numbering is stable, not contiguous: closed items are removed and their number is retired rather than reused, so a `§N` reference in a commit, ADR, or code comment keeps pointing at the same item forever.
