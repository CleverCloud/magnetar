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

| #   | Item                                                                                                                                                    | Status                 |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------- |
| 11  | [`scalable_stream_consumer` is uncallable on the tokio engine](#11-scalable_stream_consumer-is-uncallable-on-the-tokio-engine)                          | ⚡ ready to dispatch   |
| 12  | [PIP-460 per-segment consumer fan-out](#12-pip-460-per-segment-consumer-fan-out)                                                                        | ⚡ ready to dispatch   |
| 14  | [`check-sim-coverage` can report over artifacts it did not build](#14-check-sim-coverage-can-report-over-artifacts-it-did-not-build)                    | ⚡ ready to dispatch   |
| 15  | [`stalled_write_is_bounded_by_operation_timeout` flakes under load](#15-stalled_write_is_bounded_by_operation_timeout-flakes-under-load)                | ⚡ ready to dispatch   |
| 16  | [PIP-460 upstream assignment, lifecycle, DAG-ordering, and proxy contracts](#16-pip-460-upstream-assignment-lifecycle-dag-ordering-and-proxy-contracts) | 🔗 blocked on upstream |

---

## 11. `scalable_stream_consumer` is uncallable on the tokio engine

**Gap.** `PulsarClient::scalable_stream_consumer` is bound `where E::ClientState: Clone`, and **neither** engine's client implements `Clone` — not `magnetar_runtime_tokio::Client`, nor `magnetar_runtime_moonpool::Client<P>`.
The method therefore resolves on no engine at all, and no caller has ever constructed a `StreamConsumer`.
It went unnoticed because the four in-process test layers drive `magnetar_proto::Connection` directly and the e2e bodies were stubs until [ADR-0093](../specs/adr/0093-pip-460-upstream-wire-surface.md); the e2e written against a real broker is what surfaced it.

**Selected design.** The [M1-hardened StreamConsumer proposal](../specs/proposals/feat-m1-hardened-stream-consumer.md) keeps both public runtime clients non-`Clone`, gives each runtime an internal owned `SegmentSubscriber`, and makes the schema-generic aggregate consumer owned and cheap-clone over its own shared state.

**Workaround in the meantime.** The layout session is reachable directly — `lookup_scalable_topic` + `next_scalable_event` + `close_scalable_topic_session` — which is the same wire path `StreamConsumer` wraps.
`crates/magnetar/tests/e2e_scalable_topic.rs` uses exactly that.

## 12. PIP-460 per-segment consumer fan-out

**Gap.** A registered scalable consumer receives its [`ConsumerAssignment`](../specs/adr/0093-pip-460-upstream-wire-surface.md) — the `segment://` topics it owns — and the client surfaces every rebalance, but nothing attaches an ordinary consumer to those segment topics and merges their streams.
`StreamConsumer` observes the layout; it does not yet deliver messages.

**Selected design.** The [M1-hardened StreamConsumer proposal](../specs/proposals/feat-m1-hardened-stream-consumer.md) freezes assignment-driven `Exclusive` child consumers, strict locally provable DAG ordering by default, explicit broker-managed cross-member compatibility, one aggregate receive budget, source-qualified position vectors, transaction-aware acknowledgement, and an unbounded observable handoff drain.
`QueueConsumer` and `CheckpointConsumer` remain out of scope.

## 14. `check-sim-coverage` can report over artifacts it did not build

**Gap.** [ADR-0090](../specs/adr/0090-widen-sim-coverage-report-to-compiled-closure.md) split the gate into an execution step and a re-export step with **different scopes**. Execution passes `-p magnetar-runtime-moonpool -p magnetar-differential`, and `cargo llvm-cov`'s `-p` also selects which packages get _cleaned_. The report then covers all six of `SIM_COVERAGE_REPORT_PACKAGES`, so `magnetar-proto`, `magnetar-runtime-tokio`, `magnetar-auth-athenz` and `magnetar-auth-sasl` are re-exported from object files no step in the current pass is guaranteed to have produced.
CI restores the root `target/` through `Swatinem/rust-cache@v2`; the action currently prunes recognized workspace artifacts recursively, but a mutable cache action and its implementation details cannot be the coverage gate's provenance boundary.

**Why it stays open.** It was investigated as the suspected cause of the PR #391 false red and **refuted**: `cargo llvm-cov clean --workspace` followed by a cold `CARGO_INCREMENTAL=0` run reproduced the failing report exactly (81 `SF:` records, `DA:271,0`).
The real cause was optimizer inlining, fixed by [ADR-0094](../specs/adr/0094-measure-sim-coverage-unoptimized.md).
So this is a latent integrity gap with no demonstrated failure behind it, which is why it is filed rather than fixed alongside that ADR — but the direction it fails in is the fail-open one, and a gate that exists to prove patch coverage must not be able to certify coverage that did not happen.

**Selected fix.** Use one invocation-owned, initially empty llvm-cov target outside the cached workspace `target/`, and point both the execution and report phases at it.
Poisoned profile-only, object-only, and combined artifacts in the default target must then be unable to affect the report.
A missing per-file `SF:` record is not sufficient because LLVM legitimately emits none for functionless module/export/constant files; the existing record-less-_crate_ failure remains.

## 15. `stalled_write_is_bounded_by_operation_timeout` flakes under load

**Observed.** `crates/magnetar-runtime-tokio/src/driver.rs`'s `driver::tests::stalled_write_is_bounded_by_operation_timeout` (issue #370 / [ADR-0083](../specs/adr/0083-bounded-cancellable-driver-write.md)) fails intermittently under CPU pressure with `Elapsed(())` on its 90-second harness margin.

**Why that is surprising.** The test is `#[tokio::test(start_paused = true)]`, which implies the `current_thread` flavour, so its 90 seconds is _virtual_ — the failure lands in ~0.06 s of wall clock, not 90 s of it.
A paused-clock test on a single thread should be deterministic, and this one is not: it depends on host load.

**Hypothesis, not conclusion — clock-domain mismatch.** The driver computes its write deadline on the **real** clock: `use std::time::Instant` (`driver.rs:51`), `write_deadline.unwrap_or_else(|| Instant::now() + operation_timeout)` (`driver.rs:1394`), `deadline.saturating_duration_since(Instant::now())` (`driver.rs:1682`).
`tokio::time::pause()` advances tokio's timer clock; it does not advance `std::time::Instant`.
So the harness measures its margin in virtual time while the code under test measures its deadline in real time, and the two can be raced against each other by host load.
That is consistent with every observation above and it is what should be investigated first.

It is **not proven**. This entry previously asserted "a real thread, a `spawn_blocking`, or a lock held across an await"; none of those was verified, and the mixed clock domains are a better-supported explanation.
Whoever picks this up should confirm the mechanism before fixing it, not inherit this paragraph as fact.

**Measured 2026-08-04**, on `feat/pip-460-upstream-wire`:

| condition                                                                                                    | result         |
| ------------------------------------------------------------------------------------------------------------ | -------------- |
| inside `cargo test --workspace --all-features` while a full instrumented coverage rebuild saturated 16 cores | FAILED         |
| 3 isolated runs, load average still ~13                                                                      | 1 FAILED, 2 ok |
| 30 isolated runs of the test binary at idle                                                                  | 0/30 failed    |

**Ancestry: inferred, not measured.** `git diff origin/main...HEAD` touches neither the test, nor `PendingForeverStream`, nor the 90-second margin, and the three deadline lines above are byte-identical to `origin/main`; every change `feat/pip-460-upstream-wire` makes to `driver.rs` is a `#[cfg(feature = "scalable-topics")]` addition.
So both the test and the suspected mechanism predate that branch.
It has **not** been reproduced on `origin/main` under equivalent load, which is what would actually establish "pre-existing" — until someone does that, this is a well-supported inference and no more.
CI has not reproduced it on either branch.

**Why it stays open.** Filing rather than fixing is a scope call: the defect is in the driver's write path, which the PIP-460 branch does not own, and diagnosing "what escapes the paused clock" is its own investigation.
It is recorded here rather than left as folklore — a test that fails only under load is exactly the kind that gets re-run until green and then forgotten.

**Do not** fix this by widening the 90-second margin.
The margin is virtual; widening it makes the race less likely to be observed without changing anything real, which is the failure mode [ADR-0095](../specs/adr/0095-ignore-a-re-sent-scalable-layout-epoch.md) and the `lookup_error_propagation` correction both exist to avoid.

## 16. PIP-460 upstream assignment, lifecycle, DAG-ordering, and proxy contracts

**Gap.** Four contracts needed for full PIP-460 parity are absent, incomplete, or undocumented in both Pulsar 5.0.0-M1 and the inspected current `master` implementation:

1. Different consumer assignments may carry the same `layout_epoch`, but the wire has no assignment generation, controller term, or cross-connection fencing rule ([apache/pulsar#26273](https://github.com/apache/pulsar/issues/26273)).
2. A pooled Java V5 consumer has no wire unregister command; logical close removes only the local callback and can leave durable broker membership indefinitely while another pool user keeps the connection alive ([apache/pulsar#26272](https://github.com/apache/pulsar/issues/26272)).
3. The broker's assignment barrier gates active direct children but does not establish the complete PIP-460 ordering contract for sealed intermediate segments, deep DAGs, merge, or cursor rewind ([apache/pulsar#26274](https://github.com/apache/pulsar/issues/26274)).
4. Proxy-any-broker controller registration appears to select an arbitrary broker even though scalable consumer registration is leader-only, and the upstream proxy e2e exercises `QueueConsumer`, which never sends that command ([apache/pulsar#26275](https://github.com/apache/pulsar/issues/26275)).

**Interim Magnetar contract.** The [M1-hardened StreamConsumer proposal](../specs/proposals/feat-m1-hardened-stream-consumer.md) fences callbacks with a local connection incarnation, defaults to a strict barrier where local ownership history can prove every ancestor complete, reports cross-member ancestry as unprovable, documents pooled close as local-only, and fails closed when controller authority cannot be routed directly.
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

§1 closed with [ADR-0093](../specs/adr/0093-pip-460-upstream-wire-surface.md), which migrated PIP-460 onto the wire surface Apache Pulsar actually ships (vendored from 5.0.0-M1) and fleshed out the e2e against a real broker; §8 closed with [ADR-0091](../specs/adr/0091-broker-authority-default-port-unification.md) and §10 with [ADR-0092](../specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md). §11 and §12 were both surfaced by that work and became dispatch-ready once the M1-hardened StreamConsumer proposal froze their shared design. §13 closed with `e93deee`, which woke the scalable waiters on disconnect in both engines; its number is retired, which is why the entry added here is §14.
Numbering is stable, not contiguous: closed items are removed and their number is retired rather than reused, so a `§N` reference in a commit, ADR, or code comment keeps pointing at the same item forever.
