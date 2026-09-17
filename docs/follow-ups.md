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

| #   | Item                                                                                                                                                     | Status                   |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| 11  | [`scalable_stream_consumer` is uncallable on the tokio engine](#11-scalable_stream_consumer-is-uncallable-on-the-tokio-engine)                           | ⚡ ready to dispatch     |
| 12  | [PIP-460 per-segment consumer fan-out](#12-pip-460-per-segment-consumer-fan-out)                                                                         | 🧠 needs design decision |
| 14  | [`check-sim-coverage` can report over artifacts it did not build](#14-check-sim-coverage-can-report-over-artifacts-it-did-not-build)                     | ⚡ ready to dispatch     |
| 15  | [`stalled_write_is_bounded_by_operation_timeout` flakes under load](#15-stalled_write_is_bounded_by_operation_timeout-flakes-under-load)                 | ⚡ ready to dispatch     |
| 16  | [The batched `deliver` loop counts dead-lettered members as delivered](#16-the-batched-deliver-loop-counts-dead-lettered-members-as-delivered)           | 🟡 deferred              |
| 17  | [The PIP-33 marker branch returns before `maybe_flow`](#17-the-pip-33-marker-branch-returns-before-maybe_flow)                                           | 🟡 deferred              |
| 18  | [`dead_letter_pending` is unbounded and never auto-drained](#18-dead_letter_pending-is-unbounded-and-never-auto-drained)                                 | 🧠 needs design decision |
| 19  | [Re-home an established producer or consumer to another broker connection](#19-re-home-an-established-producer-or-consumer-to-another-broker-connection) | 🧠 needs design decision |
| 20  | [No deadline on a pending `ProducerOpen` / `Subscribe` request](#20-no-deadline-on-a-pending-produceropen--subscribe-request)                            | ⚡ ready to dispatch     |
| 21  | [Optional partitioned-router readiness skip](#21-optional-partitioned-router-readiness-skip)                                                             | 🧠 needs design decision |

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

**Observed.** `crates/magnetar-runtime-tokio/src/driver.rs`'s `driver::tests::stalled_write_is_bounded_by_operation_timeout` (issue #370 / [ADR-0083](../specs/adr/0083-bounded-cancellable-driver-write.md)) fails intermittently under CPU pressure with `Elapsed(())` on its 90-second harness margin.

**Why that is surprising.** The test is `#[tokio::test(start_paused = true)]`, which implies the `current_thread` flavour, so its 90 seconds is _virtual_ — the failure lands in ~0.06 s of wall clock, not 90 s of it. A paused-clock test on a single thread should be deterministic, and this one is not: it depends on host load.

**Hypothesis, not conclusion — clock-domain mismatch.** The driver computes its write deadline on the **real** clock: `use std::time::Instant` (`driver.rs:51`), `write_deadline.unwrap_or_else(|| Instant::now() + operation_timeout)` (`driver.rs:1394`), `deadline.saturating_duration_since(Instant::now())` (`driver.rs:1682`). `tokio::time::pause()` advances tokio's timer clock; it does not advance `std::time::Instant`. So the harness measures its margin in virtual time while the code under test measures its deadline in real time, and the two can be raced against each other by host load. That is consistent with every observation above and it is what should be investigated first.

It is **not proven**. This entry previously asserted "a real thread, a `spawn_blocking`, or a lock held across an await"; none of those was verified, and the mixed clock domains are a better-supported explanation. Whoever picks this up should confirm the mechanism before fixing it, not inherit this paragraph as fact.

**Measured 2026-08-04**, on `feat/pip-460-upstream-wire`:

| condition                                                                                                    | result         |
| ------------------------------------------------------------------------------------------------------------ | -------------- |
| inside `cargo test --workspace --all-features` while a full instrumented coverage rebuild saturated 16 cores | FAILED         |
| 3 isolated runs, load average still ~13                                                                      | 1 FAILED, 2 ok |
| 30 isolated runs of the test binary at idle                                                                  | 0/30 failed    |

**Ancestry: inferred, not measured.** `git diff origin/main...HEAD` touches neither the test, nor `PendingForeverStream`, nor the 90-second margin, and the three deadline lines above are byte-identical to `origin/main`; every change `feat/pip-460-upstream-wire` makes to `driver.rs` is a `#[cfg(feature = "scalable-topics")]` addition. So both the test and the suspected mechanism predate that branch. It has **not** been reproduced on `origin/main` under equivalent load, which is what would actually establish "pre-existing" — until someone does that, this is a well-supported inference and no more. CI has not reproduced it on either branch.

**Why it stays open.** Filing rather than fixing is a scope call: the defect is in the driver's write path, which the PIP-460 branch does not own, and diagnosing "what escapes the paused clock" is its own investigation. It is recorded here rather than left as folklore — a test that fails only under load is exactly the kind that gets re-run until green and then forgotten.

**Do not** fix this by widening the 90-second margin. The margin is virtual; widening it makes the race less likely to be observed without changing anything real, which is the failure mode [ADR-0095](../specs/adr/0095-ignore-a-re-sent-scalable-layout-epoch.md) and the `lookup_error_propagation` correction both exist to avoid.

## 16. The batched `deliver` loop counts dead-lettered members as delivered

**Gap.** `ConsumerState::deliver`'s batched branch increments its own `delivered` counter after every `classify_and_queue` call, including for a member the dead-letter branch routed to `dead_letter_pending` rather than to the queue.
The branch then returns `DeliverOutcome::Delivered { count: delivered }`, so `count` over-reports by the number of dead-lettered members.
`conn.rs`'s `Message` arm reads `count` as "the number of newly delivered tail entries" and clones `queue[queue_len - count ..]` to emit one `ConnectionEvent::Message` per entry, so an over-reported count makes it re-emit observational events for OLDER queued entries it already announced.

**Why it stays open.** It is adjacent to [ADR-0107](../specs/adr/0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md) and read-verified, but it changes event emission rather than flow accounting, so it wants its own test layers and its own line in the compatibility story — an application counting `ConnectionEvent::Message` sees a behaviour change.
The permit ledger is unaffected either way: `record_dispatch_unit` and the dead-letter refund are per-member and do not read `delivered`.

## 17. The PIP-33 marker branch returns before `maybe_flow`

**Gap.** `conn.rs`'s `Message` arm filters a replicated-subscription marker, calls `consumer.record_marker_consumed()` — which credits `consumed_since_flow` — and returns from the arm before reaching the `consumer.maybe_flow()` call the ordinary delivery path makes.
A marker-only stream therefore accrues refunds it never emits: the grant waits for the next non-marker frame, or for a `pop_message` that a stream carrying no user messages never gets.

**Why it stays open.** The accounting is correct (the ledger holds the credit, nothing is lost) and the practical window is a replicated subscription with no user traffic at all, so the cost is latency to the next grant rather than a wedge.
Closing it means deciding whether the marker path should emit its own flow or whether the keepalive sweep should drain the ledger, which is a small design call and the ADR-0024 five layers.

## 18. `dead_letter_pending` is unbounded and never auto-drained

**Gap.** `ConsumerState::dead_letter_pending` is a plain `Vec<IncomingMessage>` with no cap, and every caller that empties it is user-driven (`Consumer::drain_dead_letter`, `Consumer::republish_dead_letters` and the aggregate wrappers).
Before [ADR-0107](../specs/adr/0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md) the missing flow refund was an accidental bound: the subscription wedged once a receiver queue's worth of poison had accumulated, so the buffer stopped growing.
With the refund in place a poison-heavy topic keeps dispatching and the buffer grows until the application drains it.

**Why it stays open.** Needs a product decision, and the Java client offers no precedent: it has no client-side dead-letter buffer at all, it republishes to the DLQ topic and acks inside `messageReceived`.
The options are not equivalent — cap and drop (loses messages the application asked to see), cap and stop refunding (reintroduces the wedge deliberately, with a documented contract), or auto-republish when a producer is configured (changes what `dead_letter_policy` means).
There is a residual bound today regardless: a dead-lettered unit stays unacked at the broker until `republish_dead_letters` acks it, so an application that never drains eventually stops at `maxUnackedMessagesPerConsumer`.

## 19. Re-home an established producer or consumer to another broker connection

**Gap.** [ADR-0106](../specs/adr/0106-reattach-broker-closed-producer-in-place.md) re-attaches a broker-closed producer on the connection it is already pinned to, and issue #307 does the same for a consumer.
Neither can follow the bundle to a different broker.
When the `CommandCloseProducer` / `CommandCloseConsumer` carries an `assigned_broker_service_url` naming another broker, or when the retry leg's lookup answers `Redirected`, the in-place re-attach fails: `lookup_then` terminalizes the handle and the application must re-create it.
Java re-homes instead — `ConnectionHandler.grabCnx(hostUrl)` dials the assigned host, or performs a fresh lookup and takes whatever connection it resolves to.

**Why it stays open.** No proto or pool primitive exists for it.
A `Producer` / `Consumer` holds one `Arc<ConnectionShared>` for its lifetime (the engines' `open_producer` / `subscribe` capture it at creation), so re-homing means moving a live handle between two `Connection` state machines — including its pending publishes, its permit mirrors and its registered wakers.
That is an architectural decision with its own ADR, not an amendment to ADR-0106.
The current behaviour is a bounded terminal error rather than a silent hang, which is the part that mattered for issue #451.

The consumer arm's own `assigned_broker_service_url = Some(url)` branch belongs to the same decision: it still diverts to the supervised reconnect, and ADR-0106 deliberately did not transfer the producer-side argument to it without separate evidence.

## 20. No deadline on a pending `ProducerOpen` / `Subscribe` request

**Gap.** `Connection::handle_timeout` sweeps only `PendingRequestKind::Ack`.
A `CommandProducer` or `CommandSubscribe` the broker never answers leaves the slot at `broker_ready = false` (or the consumer's flow gate armed) with `open_request_id = Some(_)` forever.
That reproduces the issue #451 symptom exactly — every publish resolves `code=-1 send timeout` — and it additionally makes every LATER broker close ineligible for an in-place re-attach, because `open_request_id.is_some()` is a refusal.

**Why it stays open.** It is shared by every re-attach path — the ADR-0080 retry leg, the reconnect rebuilds, the issue #307 consumer re-subscribe and ADR-0106's producer re-attach — so it belongs to the request-deadline surface as a whole, not to any one of them.
Java covers it with `operationTimeout` applied to every pending request; the equivalent here is a per-kind deadline on `pending_requests` plus the terminalization each kind already has.

## 21. Optional partitioned-router readiness skip

**Gap.** `PartitionedProducer::pick_partition` has no readiness input: round-robin keeps handing `1/N` of all publishes to a child whose broker-side producer is detached, and those publishes wait out the whole `send_timeout` before resolving.
Issue #451's own expectation 5 asked for the router to skip such a child.

**Why it stays open.** It is beyond Java parity — `PartitionedProducerImpl.internalSendWithTxnAsync` routes through `routerPolicy.choosePartition` with no connectivity check at all, and `isConnected()` is an `allMatch` over the children — and it needs a per-slot readiness accessor on `ProducerApi`, which today exposes only the connection-level `is_connected`.
Skipping a partition also silently changes key-less ordering and per-partition distribution, which is a product decision.
ADR-0106 removes the permanent case (the child re-attaches on its own), leaving only the bounded re-attach window this would optimise.

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

§1 closed with [ADR-0093](../specs/adr/0093-pip-460-upstream-wire-surface.md), which migrated PIP-460 onto the wire surface Apache Pulsar actually ships (vendored from 5.0.0-M1) and fleshed out the e2e against a real broker; §8 closed with [ADR-0091](../specs/adr/0091-broker-authority-default-port-unification.md) and §10 with [ADR-0092](../specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md). §11 and §12 were both surfaced by that work: the first is dispatch-ready, the second needs a design decision. §13 closed with `e93deee`, which woke the scalable waiters on disconnect in both engines; its number is retired, which is why the entry added here is §14.
Numbering is stable, not contiguous: closed items are removed and their number is retired rather than reused, so a `§N` reference in a commit, ADR, or code comment keeps pointing at the same item forever.
