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

| #   | Item                                                                                | Status                                                                                                      |
| --- | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| 1   | [PIP-460 scalable-topics e2e](#1-pip-460-scalable-topics-e2e)                       | ⏳ scaffold in place; stub bodies trivially pass; flesh out once a Pulsar 5.0 RC carries PIP-460            |
| 2   | [Re-pin moonpool off git `branch = "main"`](#2-re-pin-moonpool-off-git-branch-main) | ⏳ blocked on a moonpool crates.io release carrying [PR #113](https://github.com/PierreZ/moonpool/pull/113) |
| 3   | [Log rate-limiting / sampling guidance](#3-log-rate-limiting--sampling-guidance)    | 🧠 needs design decision                                                                                    |

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

## 2. Re-pin moonpool off git `branch = "main"`

**Gap.** `Cargo.toml`'s `[workspace.dependencies]` tracks `moonpool-core` / `moonpool-sim` on `{ version = "0.6.0", git = "…", branch = "main" }` to consume the futures-io `TcpStream` + segment-granular `write_vectored` change ahead of a crates.io release.
This is a **documented, time-boxed exception** to ADR-0036's exact-pin reproducibility discipline ([ADR-0043](../specs/adr/0043-temporary-floating-moonpool-git-dep.md)).
While it stands, `cargo update -p moonpool-core` can advance the rev to an arbitrary later `main` commit; `Cargo.lock`'s concrete rev is the only reproducibility anchor.
(The `version = "0.6.0"` constraint trips resolution if `main` crosses to 0.7, surfacing the trigger automatically.)

**Why it stays open.** Blocked on upstream cutting a **moonpool crates.io release that contains [PR #113](https://github.com/PierreZ/moonpool/pull/113)**. The last published release is `0.6.0`, which predates both the futures-io migration and the vectored entry.

**`/goal` (post-release).**

```text
/goal re-pin moonpool off the git `branch = "main"` floating dependency per docs/follow-ups.md §2, once a moonpool crates.io release ships PR #113 (futures-io `NetworkProvider::TcpStream` + `SimTcpStream::poll_write_vectored`). In Cargo.toml `[workspace.dependencies]`, replace the two `{ version = "0.6.0", git = "https://github.com/PierreZ/moonpool", branch = "main" }` entries for `moonpool-core` / `moonpool-sim` with exact `=x.y.z` version pins matching the release that carries PR #113. Run `cargo update -p moonpool-core -p moonpool-sim` to refresh Cargo.lock to the released artefact. Confirm the transport still compiles against the `futures::io` ext traits (the release keeps the same surface). Remove the `[sources].allow-git` entry in deny.toml. Flip specs/adr/0043-temporary-floating-moonpool-git-dep.md Status to `Superseded by ADR-NNNN` and write the re-pin ADR (restores ADR-0036 exact-pin in full); flip the ADR-0036 amendment pointer + index status accordingly; update specs/README.md index. Update docs/moonpool-engine.md and any other version statement. Validation chain per CLAUDE.md (incl. `cargo deny check` — the release re-enables the version/advisory gates the git dep bypassed).
```

---

## 3. Log rate-limiting / sampling guidance

**Gap.** [ADR-0054](../specs/adr/0054-logging-policy.md) §7 bounds log volume structurally — per-message records are confined to `trace!`/`debug!`, and `warn!` and above are bounded by churn, never by send throughput — but defines no rate-limiting or sampling story for when the churn itself storms (e.g. a broker-restart cascade emitting one `warn!` per reconnect attempt across many connections). sozu solves this with render-time sanitization in its own logger; `tracing` has no built-in per-callsite rate limit, so the options are subscriber-side sampling (application-owned, zero library change), a documented filtering recipe, or library-side per-callsite rate limiting (which carries state per call site — exactly the "state not worth carrying for a log line" trade-off ADR-0054 leans against).

(Closed residual: the `topic`-field-presence enforcement once parked here is subsumed by the `cargo run -p xtask -- check-log-fields` gate that shipped with ADR-0054.)

(Related residual, waived in the ADR-0054 changeset as pre-existing in degree: error-`Display` fields that can embed peer-supplied text — e.g. the supervisor reconnect-failed `error = %err`, which may wrap a broker-supplied handshake reason — are not yet length-bounded; fold a normalization/truncation decision for error fields into this follow-up.)

## 4. Reconnect parity residuals (surfaced by the re-attach replay fix)

**Gap.** Fixing the `e2e_reconnect` livelock (replay/flow gated on broker acks, snapshot-window waker routing — see the `fix(proto)` commit in the ADR-0054 series) surfaced four adjacent residuals, none blocking that fix:

1. **Moonpool transient-retry arms are missing**: the moonpool driver never consumes `ProducerOpenFailedTransient` / `SubscribeFailedTransient` (the tokio driver runs the lookup-then-retry leg; moonpool has zero `Transient` matches). A post-restart broker answering a rebuild with `ServiceNotReady` dead-ends the re-attach on the moonpool engine. The `reconnect_replay_gating` twins document the asymmetry in-file.
2. **Differential harness has no connection-drop knob**: `ScriptedBroker` accepts multiple sessions but cannot script a mid-scenario drop + redial, so the re-attach replay fix carries proto-unit + 1:1 runtime-pair + e2e layers with the differential layer justified-out in the commit message. Add a `drop_connection_after(...)` injection and a reconnect equivalence scenario.
3. **Supervisor give-up semantics behind TCP-accepting proxies**: the dial-loop `max_attempts` budget only counts TCP-dial failures; post-dial handshake failures restart the cycle with `attempt = 1` (docker-proxy and any LB accept TCP while the backend is down), so the budget never fires. Count handshake failures against the same budget, resetting only on a connection that survives `drop_grace`.
4. **`e2e_reconnect` send-loop hygiene**: `producer.send().await` is unbounded by design (transparent replay keeps the future pending); the test's bounded-attempts loop only bounds `Err` returns. Wrap each attempt in `tokio::time::timeout` so an engine regression fails in seconds instead of hanging the binary.

**Why it stays open.** 1 + 2 are engine/harness features with their own ADR-0024 test obligations; 3 changes user-visible supervision semantics (needs a small design pass against Java parity); 4 is test-only polish riding whichever item lands first.

**Why it stays open.** Needs a design decision on where the mechanism lives (subscriber vs library) before any guidance is written; picking the library side adds per-callsite state and an API surface that the subscriber side gets for free.

**`/goal` (once the design question is settled).**

```text
/goal design and document rate-limiting / sampling guidance for magnetar log output per docs/follow-ups.md §3. Decide subscriber-side (document a tracing-subscriber filtering/sampling recipe in docs/logging.md, zero library change) vs library-side (per-callsite rate limiting — justify the added state against ADR-0054 §7). Land the guidance in docs/logging.md and, if the decision is binding, a short ADR-0054 amendment per specs/README.md procedure. Validation chain per CLAUDE.md (docs-only exemption applies if no code changes).
```

---

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

Two items are fully external blockers: the PIP-460 e2e flesh-out ([§1](#1-pip-460-scalable-topics-e2e)) waits on a Pulsar 5.0 RC carrying PIP-460, and the moonpool re-pin ([§2](#2-re-pin-moonpool-off-git-branch-main)) waits on a moonpool crates.io release shipping PR #113.
The logging rate-limit guidance ([§3](#3-log-rate-limiting--sampling-guidance)) waits on an internal design decision, not an external dependency.
