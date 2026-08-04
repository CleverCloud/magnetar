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

| #   | Item                                                                                                                                     | Status                   |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| 11  | [`scalable_stream_consumer` is uncallable on the tokio engine](#11-scalable_stream_consumer-is-uncallable-on-the-tokio-engine)           | ⚡ ready to dispatch     |
| 12  | [PIP-460 per-segment consumer fan-out](#12-pip-460-per-segment-consumer-fan-out)                                                         | 🧠 needs design decision |
| 14  | [`check-sim-coverage` can report over artifacts it did not build](#14-check-sim-coverage-can-report-over-artifacts-it-did-not-build)     | ⚡ ready to dispatch     |
| 15  | [`stalled_write_is_bounded_by_operation_timeout` flakes under load](#15-stalled_write_is_bounded_by_operation_timeout-flakes-under-load) | ⚡ ready to dispatch     |

---

## 11. `scalable_stream_consumer` is uncallable on the tokio engine

**Gap.** `PulsarClient::scalable_stream_consumer` is bound `where E::ClientState: Clone`, and **neither** engine's client implements `Clone` — not `magnetar_runtime_tokio::Client`, nor `magnetar_runtime_moonpool::Client<P>`.
The method therefore resolves on no engine at all, and no caller has ever constructed a `StreamConsumer`.
It went unnoticed because the four in-process test layers drive `magnetar_proto::Connection` directly and the e2e bodies were stubs until [ADR-0093](../specs/adr/0093-pip-460-upstream-wire-surface.md); the e2e written against a real broker is what surfaced it.

**Why it stays open.** The fix is a small API decision rather than a bug fix: either make both clients cheap-clone (each is already `Arc`-backed internally, so this is close to a `derive`), or drop the `Clone` bound and have `StreamConsumer` hold a borrow or an `Arc` of the client. Both change a published signature, so it wants a deliberate choice rather than the first thing that compiles.

**Workaround in the meantime.** The layout session is reachable directly — `lookup_scalable_topic` + `next_scalable_event` + `close_scalable_topic_session` — which is the same wire path `StreamConsumer` wraps. `crates/magnetar/tests/e2e_scalable_topic.rs` uses exactly that.

## 12. PIP-460 per-segment consumer fan-out

**Gap.** A registered scalable consumer receives its [`ConsumerAssignment`](../specs/adr/0093-pip-460-upstream-wire-surface.md) — the `segment://` topics it owns — and the client surfaces every rebalance, but nothing attaches an ordinary consumer to those segment topics and merges their streams.
`StreamConsumer` observes the layout; it does not yet deliver messages.

**Why it stays open.** Needs a design decision on ordering across segments, on how per-segment cursors interact with the single subscription name, and on what happens to in-flight messages at a rebalance. `QueueConsumer` and `CheckpointConsumer` sit behind the same decision, and ADR-0093 deliberately left all three out of scope.

## 14. `check-sim-coverage` can report over artifacts it did not build

**Gap.** [ADR-0090](../specs/adr/0090-widen-sim-coverage-report-to-compiled-closure.md) split the gate into an execution step and a re-export step with **different scopes**.
Execution passes `-p magnetar-runtime-moonpool -p magnetar-differential`, and `cargo llvm-cov`'s `-p` also selects which packages get _cleaned_.
The report then covers all six of `SIM_COVERAGE_REPORT_PACKAGES`, so `magnetar-proto`, `magnetar-runtime-tokio`, `magnetar-auth-athenz` and `magnetar-auth-sasl` are re-exported from object files no step in the current pass is guaranteed to have produced.
CI compounds it: `Swatinem/rust-cache@v2` in the `check-sim-coverage` job runs unconfigured, so it archives `target/` — including `target/llvm-cov-target`, which its workspace-artifact pruning does not know about.

**Why it stays open.** It was investigated as the suspected cause of the PR #391 false red and **refuted**: `cargo llvm-cov clean --workspace` followed by a cold `CARGO_INCREMENTAL=0` run reproduced the failing report exactly (81 `SF:` records, `DA:271,0`).
The real cause was optimizer inlining, fixed by [ADR-0094](../specs/adr/0094-measure-sim-coverage-unoptimized.md).
So this is a latent integrity gap with no demonstrated failure behind it, which is why it is filed rather than fixed alongside that ADR — but the direction it fails in is the fail-open one, and a gate that exists to prove patch coverage must not be able to certify coverage that did not happen.

**Candidate fixes, cheapest first.** Treat a file inside a gated crate that carries added lines but emits **no** `SF:` record as a hard failure, extending the existing record-less-_crate_ bail to per-file granularity — free, and it turns "could not measure" into a red instead of a silent pass.
Failing that, `cargo llvm-cov clean --workspace` before the execution step, which is correct but pays a full instrumented rebuild — including `aws-lc-fips-sys` — on every run and defeats the job's cache.

## 15. `stalled_write_is_bounded_by_operation_timeout` flakes under load

**Gap.** `crates/magnetar-runtime-tokio/src/driver.rs`'s `driver::tests::stalled_write_is_bounded_by_operation_timeout` (issue #370 / [ADR-0083](../specs/adr/0083-bound-the-driver-write-on-operation-timeout.md)) fails intermittently under CPU pressure with `Elapsed(())` on its 90-second harness margin.

**Why that is surprising.** The test is `#[tokio::test(start_paused = true)]`, which implies the `current_thread` flavour, so the 90 seconds is _virtual_ — the failure lands in ~0.06 s of wall clock, not 90 s of it. Under `start_paused` tokio auto-advances the clock whenever no task is ready, so a deterministic test should resolve the driver's own 30 s `operation_timeout` long before the 90 s margin. That it does not, and that it depends on host load, means something on the driver's stalled-write path leaves the virtual clock's control — a real thread, a `spawn_blocking`, or a lock held across an await. That is worth knowing independently of the test.

**Measured 2026-08-04**, on `feat/pip-460-upstream-wire`:

| condition                                                                                                    | result         |
| ------------------------------------------------------------------------------------------------------------ | -------------- |
| inside `cargo test --workspace --all-features` while a full instrumented coverage rebuild saturated 16 cores | FAILED         |
| 3 isolated runs, load average still ~13                                                                      | 1 FAILED, 2 ok |
| 30 isolated runs of the test binary at idle                                                                  | 0/30 failed    |

**Not caused by the PIP-460 work.** `git diff origin/main...HEAD` touches neither the test, nor `PendingForeverStream`, nor the 90-second margin; every change to `driver.rs` on that branch is a `#[cfg(feature = "scalable-topics")]` addition. CI has not reproduced it.

**Why it stays open.** Filing rather than fixing is a scope call: the defect is in the driver's write path, which the PIP-460 branch does not own, and diagnosing "what escapes the paused clock" is its own investigation. It is recorded here rather than left as folklore — a test that fails only under load is exactly the kind that gets re-run until green and then forgotten.

**Do not** fix this by widening the 90-second margin. The margin is virtual; widening it makes the race less likely to be observed without changing anything real, which is the failure mode [ADR-0095](../specs/adr/0095-ignore-a-re-sent-scalable-layout-epoch.md) and the `lookup_error_propagation` correction both exist to avoid.

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

§1 closed with [ADR-0093](../specs/adr/0093-pip-460-upstream-wire-surface.md), which migrated PIP-460 onto the wire surface Apache Pulsar actually ships (vendored from 5.0.0-M1) and fleshed out the e2e against a real broker; §8 closed with [ADR-0091](../specs/adr/0091-broker-authority-default-port-unification.md) and §10 with [ADR-0092](../specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md). §11 and §12 were both surfaced by that work: the first is dispatch-ready, the second needs a design decision. §13 closed with `e93deee`, which woke the scalable waiters on disconnect in both engines; its number is retired, which is why the entry added here is §14.
Numbering is stable, not contiguous: closed items are removed and their number is retired rather than reused, so a `§N` reference in a commit, ADR, or code comment keeps pointing at the same item forever.
