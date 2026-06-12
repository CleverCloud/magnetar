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

| #   | Item                                                                                                                | Status                                                                                           |
| --- | ------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| 1   | [PIP-460 scalable-topics e2e](#1-pip-460-scalable-topics-e2e)                                                       | ⏳ scaffold in place; stub bodies trivially pass; flesh out once a Pulsar 5.0 RC carries PIP-460 |
| 2   | [Log rate-limiting / sampling guidance](#2-log-rate-limiting--sampling-guidance)                                    | 🧠 needs design decision                                                                         |
| 3   | [Reconnect parity residuals](#3-reconnect-parity-residuals-surfaced-by-the-re-attach-replay-fix)                    | 🧠 engine/harness features + a supervision-semantics design pass                                 |
| 4   | [Survivability residuals (ADR-0055 bit-flip fix)](#4-survivability-residuals-surfaced-by-the-adr-0055-bit-flip-fix) | 🟡 2 engine residuals (fast-fail, lookup-retry) + 3 pre-existing test-state caveats              |
| 5   | [Residuals from the moonpool seed-sweep fixes](#5-residuals-surfaced-by-the-moonpool-seed-sweep-fixes)              | ⚡ marker lost-wakeup race (latent) + a single-provider tls-chaos build gap                      |

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

(Closed residual: the `topic`-field-presence enforcement once parked here is subsumed by the `cargo run -p xtask -- check-log-fields` gate that shipped with ADR-0054.)

(Related residual, waived in the ADR-0054 changeset as pre-existing in degree: error-`Display` fields that can embed peer-supplied text — e.g. the supervisor reconnect-failed `error = %err`, which may wrap a broker-supplied handshake reason — are not yet length-bounded; fold a normalization/truncation decision for error fields into this follow-up.)

## 3. Reconnect parity residuals (surfaced by the re-attach replay fix)

**Gap.** Fixing the `e2e_reconnect` livelock (replay/flow gated on broker acks, snapshot-window waker routing — see the `fix(proto)` commit in the ADR-0054 series) surfaced four adjacent residuals, none blocking that fix:

1. **Moonpool transient-retry arms are missing**: the moonpool driver never consumes `ProducerOpenFailedTransient` / `SubscribeFailedTransient` (the tokio driver runs the lookup-then-retry leg; moonpool has zero `Transient` matches).
   A post-restart broker answering a rebuild with `ServiceNotReady` dead-ends the re-attach on the moonpool engine.
   The `reconnect_replay_gating` twins document the asymmetry in-file.
2. **Differential harness has no connection-drop knob**: `ScriptedBroker` accepts multiple sessions but cannot script a mid-scenario drop + redial, so the re-attach replay fix carries proto-unit + 1:1 runtime-pair + e2e layers with the differential layer justified-out in the commit message.
   Add a `drop_connection_after(...)` injection and a reconnect equivalence scenario.
3. **Supervisor give-up semantics behind TCP-accepting proxies**: the dial-loop `max_attempts` budget only counts TCP-dial failures; post-dial handshake failures restart the cycle with `attempt = 1` (docker-proxy and any LB accept TCP while the backend is down), so the budget never fires.
   Count handshake failures against the same budget, resetting only on a connection that survives `drop_grace`.

(Closed residual: the `e2e_reconnect` send-loop hygiene gap — unbounded `send().await` turning environmental broker death into an infinite hang — was fixed in the same series after a crashed standalone container hung the validation chain for 20 hours; each send attempt is now timeout-bounded.)

**Why it stays open.** 1 + 2 are engine/harness features with their own ADR-0024 test obligations; 3 changes user-visible supervision semantics (needs a small design pass against Java parity).

**Why it stays open.** Needs a design decision on where the mechanism lives (subscriber vs library) before any guidance is written; picking the library side adds per-callsite state and an API surface that the subscriber side gets for free.

**`/goal` (once the design question is settled).**

```text
/goal design and document rate-limiting / sampling guidance for magnetar log output per docs/follow-ups.md §2. Decide subscriber-side (document a tracing-subscriber filtering/sampling recipe in docs/logging.md, zero library change) vs library-side (per-callsite rate limiting — justify the added state against ADR-0054 §7). Land the guidance in docs/logging.md and, if the decision is binding, a short ADR-0054 amendment per specs/README.md procedure. Validation chain per CLAUDE.md (docs-only exemption applies if no code changes).
```

---

## 4. Survivability residuals (surfaced by the ADR-0055 bit-flip fix)

**Finding.** PR #218's `seed-replay` failure was not `a02f401`'s logic and not a moonpool delivery bug.
It was moonpool's default-on FoundationDB bit-flip chaos corrupting a Pulsar _command_ frame — which TCP would never deliver in production, since only message payloads carry CRC32C — and `a02f401`'s write-schedule shift happened to land that flip on the two ADR-0038 anchor seeds.
[ADR-0055](../specs/adr/0055-bit-flip-survivability-model.md) makes corruption _survivable_ instead of disabling the chaos: a plain connection fails its in-flight ops fast (`PeerClosed`) instead of hanging, and the chaos workloads run supervised over a broker that persists its ledger + per-subscription cursor across reconnects.
Both anchor seeds (`0x56201ccaba82dbc1`, `0xdc638c565234d23f`) are green.

**Gap.** Two engine residuals, neither blocking the fix:

1. **Terminal-state fast-fail for NEW ops.** `Connection::fail_all_pending` only terminalizes ops that were already pending AT the drop.
   A `producer.send()` / `subscribe()` / `producer.close()` issued AFTER a plain connection is already terminal (`Failed` / `Closed`) still registers a doomed pending op that hangs — there is no driver left to resolve it.
   ADR-0055 §1 is scoped to the in-flight contract, so the terminal-exit / differential / e2e tests deliberately do not assert on this.
   Fix: a synchronous fast-fail in the send / request-issue / subscribe paths that returns `PeerClosed` immediately when the handshake state is `Failed` / `Closed`.
2. **Lookup `SessionLost` is not transparently re-issued.** A transient `SessionLost` on the in-flight `CommandLookupTopic` behind `subscribe` / `open_producer` during a supervised reconnect surfaces to the caller as `ClientError::Other` — the engine does not auto-reissue the lookup the way it transparently replays producer sends and re-subscribes.
   The `sim_chaos` workloads retry around it at setup, but a production caller subscribing/opening _during_ a reconnect could see this `Other`.
   Fix: engine-side lookup-retry-on-`SessionLost` (mirrors Java's lookup-after-reset), with its own ADR-0024 layers.

**Test-state caveats** — NOT caused by this change (the diff touches no `magnetar-admin` or replicated-subscription code), flagged so the next reader is not surprised that a full `--all-features` run is not 100% green:

- **`e2e_admin_topic_policies_breadth` fails on `apachepulsar/pulsar:4.0.4`** (a `retention = -1` round-trip).
  Pre-existing — reproduces on the base branch, unrelated to ADR-0055.
  It keeps the full `cargo test --workspace --all-features` run (and the per-PR `test` CI job) red until addressed separately; this is a `magnetar-admin` / Pulsar-version concern, not a survivability one.
- **Seed-13 `replicated_subscriptions::consumer_emits_marker_observation_in_order` flake.** Pre-existing seed-flakiness (passes on re-run and in isolation).
  The deterministic `sim_chaos` surface this change edits is clean on every seed `1..32`.
  See [§5.1](#5-residuals-surfaced-by-the-moonpool-seed-sweep-fixes) — this flake is plausibly the latent lost-wakeup race in the marker accessor, surfaced by the seed-sweep work.
- **`check-sim-coverage` reports ~77 uncovered lines.** The diff is computed vs `origin/main`, so it bundles the prior terminal-outcome commit plus line-number-shift artifacts of pre-existing code; the behavioral lines this change adds (the `Closed → PeerClosed` waiter mappings, the decode-fatal broker hook, the fatal-on-send arm) are exercised by the new differential + integration tests.
  The gate is local-first / scheduled-CI (it short-circuits on `main`); dispatch it from a feature branch for true patch gating once `feat/logging` lands on `main`.

**Partial progress on [§3.2](#3-reconnect-parity-residuals-surfaced-by-the-re-attach-replay-fix).** This change added a corrupt-frame injection to the differential `ScriptedBroker` (`inject_decode_fatal_frame_on_send`).
The mid-scenario **drop + redial** knob §3.2 asks for is still open.

**Why it stays open.** §4.1 + §4.2 are engine features with their own ADR-0024 test obligations and a small Java-parity design pass; the test-state caveats are pre-existing suite issues / gate mechanics, not survivability work.

**`/goal` (engine fast-fail residual §4.1).**

```text
/goal implement terminal-state fast-fail for NEW operations per docs/follow-ups.md §4.1. When a plain connection is already terminal (handshake state Failed/Closed), the send / request-issue / subscribe paths must return ClientError::PeerClosed synchronously instead of registering a pending op that never resolves. Mirror across both engines (ADR-0024 1:1) with proto unit + tokio/moonpool integration twins + a differential layer. Validation chain per CLAUDE.md.
```

---

## 5. Residuals surfaced by the moonpool seed-sweep fixes

Found while reproducing and fixing the daily-sweep `seed-failure` issues (the `fix/moonpool-seed-sweep-fixes` series: post-dial handshake timeout, progress-based keepalive watchdog [ADR-0058](../specs/adr/0058-keepalive-watchdog-progress-based.md), anti-thrash cooldown gating, memory-limit live-connection gating).
Neither residual blocks that series.

1. **Replicated-subscription marker accessor lost-wakeup race (latent).**
   `Client::next_replicated_subscription_marker` (tokio `crates/magnetar-runtime-tokio/src/client.rs`, moonpool `crates/magnetar-runtime-moonpool/src/client.rs`) loops `pop_front()` → `is_closed()` → `notified().await`, enrolling the `Notify` waiter _after_ the empty check; the driver pushes the observation then calls `notify_waiters()`, which stores no permit, so a marker delivered in that gap is lost and the future hangs.
   This is the exact shape already fixed for `SubscribeAckedFut` at `crates/magnetar-runtime-moonpool/src/consumer.rs:1494-1505`.
   It is real by inspection but not currently seed-reproducible: the `replicated_subscriptions` suite runs over real-TCP `TokioProviders`, not `SimProviders`, so it is non-deterministic and never drives the parked-waiter gap — which is why issue #157's seed passes on `main` and the [§4](#4-survivability-residuals-surfaced-by-the-adr-0055-bit-flip-fix) "seed-13 marker flake" caveat only manifests intermittently.
   Fix: the enroll-before-drain idiom already used at `producer.rs:510-513`, mirrored 1:1 across both engines; a deterministic regression test needs a new `SimulationBuilder` / `SimProviders`-driven `replicated_subscriptions` harness with a delayed-marker broker.

2. **`tls_handshake_chaos.rs` hardcodes the ring crypto provider (build gap).**
   `crates/magnetar-runtime-tokio/tests/tls_handshake_chaos.rs:23` calls `rustls::crypto::ring::default_provider()` with no `#[cfg(feature = "crypto-ring")]` gate, so `cargo build` / `test` / `clippy -p magnetar-runtime-tokio --no-default-features --features crypto-aws-lc-rs` fails to compile (`E0433`) — the single-provider feature set the moonpool sweep and the per-PR `seed-replay` job use.
   Pre-existing (reproduces on the base branch); it blocks a single-provider tokio test build but is unrelated to any seed fix.
   Fix: gate the test on `crypto-ring`, or derive the provider from the active feature like the rest of the tokio TLS surface.

**Partial progress on [§3.3](#3-reconnect-parity-residuals-surfaced-by-the-re-attach-replay-fix).**
The handshake-timeout fix bounds the post-dial CONNECT→CONNECTED handshake by `operation_timeout` (surfacing `Io(TimedOut)` instead of hanging when a broker accepts TCP but never answers `CommandConnect`), so a wedged handshake now fails fast; the §3.3 budget-counting residual (handshake failures restart the dial cycle with `attempt = 1`) is unchanged.

**Why it stays open.** §5.1 is an engine concurrency fix with its own ADR-0024 obligations plus a new SimProviders harness; §5.2 is a one-line test feature-gate.

**`/goal` (marker lost-wakeup §5.1).**

```text
/goal fix the replicated-subscription marker accessor lost-wakeup race per docs/follow-ups.md §5.1. Move the replicated_subscription_marker_notify.notified() enrollment BEFORE the pop_front()/is_closed() drain in next_replicated_subscription_marker in both engines (the producer.rs:510-513 enroll-before-drain idiom), keeping tokio and moonpool at 1:1. Ship the four ADR-0024 layers INCLUDING a new SimProviders/SimulationBuilder-driven replicated_subscriptions harness with a delayed-marker broker that deterministically parks the waiter before the marker arrives. Validation chain per CLAUDE.md.
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
