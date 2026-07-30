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

| #   | Item                                                                                                          | Status                                                                                           |
| --- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| 1   | [PIP-460 scalable-topics e2e](#1-pip-460-scalable-topics-e2e)                                                 | ⏳ scaffold in place; stub bodies trivially pass; flesh out once a Pulsar 5.0 RC carries PIP-460 |
| 2   | [Client-driven rate-window sampling](#2-client-driven-rate-window-sampling)                                   | ⚡ ready to dispatch                                                                             |
| 8   | [Broker-URL authority parser unification — residual](#8-broker-url-authority-parser-unification--residual)    | 🟡 deferred (three of four parsers unified; `parse_direct_broker_url` audited, not unified)      |
| 10  | [`check-sim-coverage` never instruments `magnetar-proto`](#10-check-sim-coverage-instruments-only-two-crates) | 🧠 needs a decision on how to broaden the coverage run                                           |

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

## 2. Client-driven rate-window sampling

**Gap.** `ConsumerStats::msgs_per_sec` / `bytes_per_sec` (and the `ProducerStats` pair) only become nonzero after a second `record_rate_window` call on the same state object, and **nothing in either engine ever calls it** — sampling is 100% caller-driven.
The production entry points are `magnetar_proto::Connection::{consumer,producer}_record_rate_window` (`crates/magnetar-proto/src/conn.rs:4670`, `:4678`) and the two tokio pass-throughs (`crates/magnetar-runtime-tokio/src/producer.rs:644`, `consumer.rs:918`); every caller in the tree is a test.
So the `aggregate_stats()` rate fields — structurally correct since the #347 fold fix — sum zeros for every wrapper type.
Surfaced while writing `crates/magnetar/tests/e2e_aggregate_stats.rs`, whose header (`:12-17`) defers the rate assertion for exactly this reason.

**Reachability today.**

| Surface                                                                                                      | Children                                                                                                                                 | Can a caller drive `record_rate_window`?                                                                                                                                                                                   |
| ------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PartitionedProducer` (`crates/magnetar/src/partitioned_producer.rs:276`)                                    | `partitions: Vec<P>`, public accessor `child_producers() -> &[P]` (`:311`)                                                               | **yes, on the tokio engine** — the façade re-exports `magnetar_runtime_tokio` (`crates/magnetar/src/lib.rs:60`), so `for p in pp.child_producers() { p.record_rate_window(now) }` compiles today                           |
| `MultiTopicsConsumer` + the `PartitionedConsumer` alias (`multi_topics.rs:57`, `partitioned_consumer.rs:27`) | `Mutex<Arc<Vec<NamedConsumer<C>>>>` (`multi_topics.rs:71`); no public accessor, private `lookup()` (`:676`) hands out one child by topic | no                                                                                                                                                                                                                         |
| `PatternConsumer` (`pattern_consumer.rs:54`)                                                                 | same copy-on-write shape                                                                                                                 | no                                                                                                                                                                                                                         |
| moonpool `Producer<P>` / `Consumer<P>`                                                                       | —                                                                                                                                        | **no, at any layer** — `record_rate_window` does not exist on either type (zero occurrences in `crates/magnetar-runtime-moonpool/`), though the underlying `magnetar_proto` state that carries the method is the same type |
| `ProducerApi` / `ConsumerApi` (`crates/magnetar/src/engine/mod.rs:265`, `:394`)                              | —                                                                                                                                        | not on the traits — `stats` (`:311`, `:483`) and `{send,receive}_latency_histogram` (`:321`, `:494`) are, the rate-window tick is not, so no engine-generic fan-out can be written without lifting it first                |

**What the Java client does.** The parity target drives this from the leaf, not the wrapper.

| Java site                                                                   | Behaviour                                                                                                                                                                  |
| --------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ProducerStatsRecorderImpl` / `ConsumerStatsRecorderImpl`                   | schedules `pulsarClient.timer().newTimeout(stat, statsIntervalSeconds, SECONDS)` and re-arms itself in the task's `finally` — the leaf self-ticks on the client-wide timer |
| `ClientConfigurationData.statsIntervalSeconds`                              | `60` by default (`ClientConfigurationData.java:144`, apache/pulsar `master`); `0` disables the recorders entirely                                                          |
| `PartitionedProducerImpl.getStats()` / `MultiTopicsConsumerImpl.getStats()` | `stats.reset()` then fold each child's `getStats()` — **no timer of their own, and no per-child tick**                                                                     |

magnetar's `aggregate_stats()` (`partitioned_producer.rs:390`, `multi_topics.rs:459`) is already the exact analogue of Java's wrapper `getStats()`, down to summing the rates as f64 (`ProducerStats::fold`, `crates/magnetar-proto/src/producer.rs:468-469`; `ConsumerStats::fold`, `consumer.rs:618-619`).
What is missing is not a wrapper surface Java does not have — it is the leaf-level tick Java does have.
A fan-out method on the wrappers would compensate at the wrong layer and ship a magnetar-only divergence.

**Decision (2026-07-30).** Drive the tick from the sans-io core as a deadline source, and add no wrapper API at all.

`record_rate_window` is the only periodic obligation in the client not expressed as a deadline: keepalive, the nack / unacked / ack-grouping trackers, chunk expiry, receiver-queue auto-adjust, batch flush, send timeout, relocated in-flight sends and `ack_response_timeout` are all armed in `Connection::poll_timeout` (`crates/magnetar-proto/src/conn.rs:3660-3745`) and swept in `Connection::handle_timeout` (`:3750-4057`).
That loop is magnetar's structural equivalent of Java's client-wide `HashedWheelTimer`.
Arming the rate window there behind `ConnectionConfig::stats_interval` costs no new state (each slot's existing `last_rate_snapshot` is the per-slot baseline), no new task, no new `select!` arm, and emits no frame or `ConnectionEvent` — so the golden `EventStream` traces are untouched and the sweep is bit-for-bit reproducible under moonpool, whose driver already passes a virtual `now` (`crates/magnetar-runtime-moonpool/src/driver.rs:1706`).
Every wrapper's children are slots on some connection, so `PartitionedProducer`, `MultiTopicsConsumer`, `PatternConsumer` and any future aggregator all get correct rates without a line of new façade code.

Runner-up, worth a follow-on commit but not a substitute: lift `record_rate_window` onto `ProducerApi` / `ConsumerApi` and add the two missing moonpool pass-throughs, giving tests an explicit synchronous sample point the opportunistic sweep cannot offer.
That is a breaking trait change (`BREAKING CHANGE:` footer per this file's API-stability stance) and it does not on its own make the wrappers correct — see trap 4.

**Rejected alternatives.**

| Option                                                                                     | Why not                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tokio-only carve-out `impl` blocks on the wrappers                                         | [ADR-0037](../specs/adr/0037-multi-topics-pattern-consumer-pass-2-lift.md) is the record of removing exactly that shape from these two types ("No tokio-only carve-out is needed"), and the moonpool engine would still gain nothing                                                                                                                                                                                                                                                                                                                                                                                                                  |
| A public child accessor on `MultiTopicsConsumer` only                                      | exports child clone semantics ([ADR-0057](../specs/adr/0057-producer-last-clone-drop-close.md), [ADR-0077](../specs/adr/0077-consumer-last-clone-drop-close.md)) to user code purely to work around a missing engine method, and it cannot mirror `child_producers()` anyway — the children sit behind a `Mutex<Arc<Vec<_>>>`, so the signature has to be a snapshot clone                                                                                                                                                                                                                                                                            |
| An auto-ticker knob on the wrapper builders, shaped like `auto_update_partitions_interval` | determinism-hostile and currently unimplementable: `MoonpoolEngine::new_interval` is literally `tokio::time::interval` (`crates/magnetar/src/engine/moonpool.rs:88`) and `spawn` is `tokio::spawn`, but [ADR-0078](../specs/adr/0078-adopt-moonpool-0-8-native-deterministic-runtime.md) runs `SimProviders` workloads on Moonpool's own deterministic executor with no ambient tokio runtime and lists code depending on `tokio::time` as incompatible with the engine. `Engine::spawn` / `new_interval` are static with no providers argument (`engine/mod.rs:106`, `:116`), so fixing them is an `Engine`-trait signature change, not an impl swap |
| Read-triggered sampling — `aggregate_stats(now)` ticks then folds                          | two readers at different cadences destroy each other's window (each call re-seeds `last_rate_snapshot`), it makes a `&self` getter mutate observable state, and it breaks an existing published signature on two types                                                                                                                                                                                                                                                                                                                                                                                                                                |

**Traps for the implementer.**

1. A ticker holding a clone of a child keeps that child's `Arc<ProducerCloseGuard>` alive, so the fire-and-forget `CommandCloseProducer` never fires and issue #241 regresses ([ADR-0057](../specs/adr/0057-producer-last-clone-drop-close.md)).
2. A ticker holding an `Arc<Inner>` pushes `MultiTopicsConsumer::close` into its documented "clones outlive us" branch (`multi_topics.rs:540-544`), which returns `Ok(())` without closing a single child.
3. `PartitionedProducer` is not `Clone` and owns its `Vec<P>` inline, so any `'static` handle to its children means restructuring a published type.
4. `fold` sums the rates as plain f64 with no window metadata, so a caller who ticks 3 of 4 children — or ticks them at different cadences — gets an authoritative-looking total that is meaningless.
   One clock ticking every slot is what makes the sum well-defined.
5. A child added mid-window (`add_topic`, partition growth) has no baseline yet, so its first tick only seeds one and it contributes `0.0` for a full interval. Java has the identical property, so this is parity-correct — but document it on `aggregate_stats`.

**`/goal`.**

```text
/goal implement client-driven rate-window sampling per docs/follow-ups.md §2. Add `stats_interval: Option<Duration>` to `magnetar_proto::ConnectionConfig` (default `None`), armed in `Connection::poll_timeout` alongside the existing per-slot loops and swept in `Connection::handle_timeout` where each `slot.state.lock()` is already held, gated exactly like the `ack_response_timeout` arm at crates/magnetar-proto/src/conn.rs:3733-3742 — skip the deadline entirely when the knob is `None`, because an armed-but-never-firing deadline still perturbs the moonpool wake schedule. Use each slot's existing `last_rate_snapshot` timestamp as the per-slot baseline; add no new state. Expose it as `ClientBuilder::stats_interval` mirroring the keepalive knob at crates/magnetar/src/client_builder.rs:184. Add NO fan-out method to PartitionedProducer / MultiTopicsConsumer / PatternConsumer: Java's wrappers have none (PartitionedProducerImpl.getStats() only resets and folds children; the leaves self-tick on pulsarClient.timer()), so a wrapper surface would be a magnetar-only divergence. Ship the ADR-0024 four-layer test set plus the e2e, delete the disclaimer at crates/magnetar/tests/e2e_aggregate_stats.rs:12-17 and turn on the rate assertion it defers. Land a new ADR recording the decision and flip the README statsInterval parity row to ✅. Keep the default `None` in this commit; a follow-up commit flips it to `Some(60s)` for Java parity (ClientConfigurationData.java:144) only after a clean 1..32 moonpool seed sweep, so a seed regression bisects to one line. Validation chain per CLAUDE.md.
```

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

## 10. `check-sim-coverage` instruments only two crates

**Gap.** [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md) requires 100% moonpool coverage on the diff, and `cargo run -p xtask -- check-sim-coverage` is the gate.
Its LCOV report covers **only** `crates/magnetar-runtime-moonpool/src/**` and `crates/magnetar-differential/src/**`.

Measured 2026-07-30 on this branch — `target/sim-coverage.lcov` carried 16 `SF:` records, 12 moonpool + 4 differential, and nothing else:

```
rg -o '^SF:.*' target/sim-coverage.lcov | sed 's|^SF:.*/crates/||' | cut -d/ -f1 | sort | uniq -c
     4 magnetar-differential
    12 magnetar-runtime-moonpool
```

`run_moonpool_lcov` filters with `-p magnetar-runtime-moonpool -p magnetar-differential`, and the report carries those two packages' own sources rather than their dependency closure.
So **`magnetar-proto` has never been gated** — the crate invariant #9 in [`CLAUDE.md`](../CLAUDE.md) singles out — nor has `magnetar-runtime-tokio`, nor the `magnetar` façade.

Before [ADR-0088](../specs/adr/0088-sim-coverage-gate-scope-report-ungated-additions.md) this was invisible: a file with no LCOV entry has no `DA:` records, so every added line in it read as "not executable" and the run printed "all added lines are covered" over files it had never measured.
The gate now prints `not gated (outside the moonpool coverage run): <path>: N added line(s)` for them and still exits 0, which is what surfaced the proto extent.

**Why it stays open.** Fixing the scope is a rework of the gate's mechanics, not a flag change, and the options differ in cost and blast radius:

| Option                                                                                                    | Trade-off                                                                                                                                                                                                                                                     |
| --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cargo llvm-cov --workspace` with execution still restricted to the moonpool + differential test binaries | closest to ADR-0024's intent — report on everything, execute only the sim surface. Needs the right `--workspace` + test-target selection so no tokio-only or Docker-bound target runs.                                                                        |
| per-package `--no-report` runs stitched with `llvm-cov report`                                            | precise control over which crates are reported, at the cost of a multi-step invocation and a longer run.                                                                                                                                                      |
| add `-p magnetar-driver`                                                                                  | reaches the façade, but per [ADR-0046](../specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) its `tests/e2e_*.rs` carry no feature gate and no `#[ignore]`, so every coverage run would need Docker and a live `apachepulsar/pulsar` container. |

Whichever lands, expect the gate to start failing on real, previously-invisible gaps — so it needs a deliberate first run on `main` to size the backlog before it becomes hard-failing.
Until then, ADR-0024 on proto changes is carried by the four-layer test policy and review, not by this gate.

**`/goal`.**

```text
/goal broaden check-sim-coverage to actually gate magnetar-proto per docs/follow-ups.md §10. Today run_moonpool_lcov in xtask/src/main.rs filters with `-p magnetar-runtime-moonpool -p magnetar-differential` and the emitted target/sim-coverage.lcov carries only those two crates' own sources (16 SF records, measured 2026-07-30), so ADR-0024's 100%-patch-coverage requirement has never been enforced on magnetar-proto, magnetar-runtime-tokio, or the magnetar façade. First reproduce the scope with `rg -o '^SF:.*' target/sim-coverage.lcov` and record the baseline count. Then rework the invocation so the REPORT covers the dependency closure while EXECUTION stays restricted to the moonpool + differential test binaries — prefer a `--workspace` run with explicit test-target selection, or per-package `--no-report` runs stitched with `llvm-cov report`; do NOT add `-p magnetar-driver` if that pulls the Docker-bound e2e suite (ADR-0046) into every run. Prove the fix by adding a deliberately-uncovered line to crates/magnetar-proto/src/ and showing the gate FAILS on it, then removing it and showing the gate passes — a gate never seen red detects nothing. Run it once against main to size the pre-existing backlog and report the count before deciding whether it hard-fails immediately or lands with a documented allowlist. Update ADR-0088's measured-scope paragraph, the run_moonpool_lcov doc comment, and CLAUDE.md invariant #9. Validation chain per CLAUDE.md.
```

---

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

§1 is a fully external blocker (the PIP-460 e2e flesh-out waits on a Pulsar 5.0 RC carrying PIP-460); §8 is trimmed to one audited-not-unified parser whose closure is an API-shape question; §10 needs a call on how to broaden the coverage run before it can be dispatched; §2 is dispatch-ready.
Numbering is stable, not contiguous: closed items are removed and their number is retired rather than reused, so a `§N` reference in a commit, ADR, or code comment keeps pointing at the same item forever.
