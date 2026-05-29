# Open Follow-Ups

Consolidated tracker for known open work. Each entry lists the gap,
the reason it stays open, and (where actionable) a `/goal …` block
ready to be copy-pasted verbatim into a fresh session for an agent
team to pick up.

For the public-facing parity status, see
[`parity-status.md`](parity-status.md) and the
[parity matrix in the README](../README.md#java-client-parity-matrix).

This file is the **single source of truth** for what is intentionally
deferred or blocked. Anything not listed below is either landed
(check `git log` for the implementation reference), or explicitly out
of scope for v0.2.0 ([ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
§D-series, [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md),
[ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)).

When a PR lands an item, the entry is **removed** (git log + the ADR /
docs file carry the post-implementation reference); partially-landed
items are trimmed to their remaining open residual.

**API stability stance.** The crate is not yet published. Breaking
API changes are acceptable when they improve correctness, ergonomics,
or layering; ship with `BREAKING CHANGE:` in the commit body so the
eventual changelog flags them.

---

## Index

Status tags: ⚡ ready to dispatch · 🔗 blocked on external dep ·
⏳ blocked on upstream PIP release · 🧠 needs design decision ·
🟡 deferred (not load-bearing).

| # | Item | Status |
| - | --- | --- |
| 1 | [PIP-460 scalable-topics e2e](#1-pip-460-scalable-topics-e2e) | ⏳ scaffold landed; e2e blocked on a Pulsar 5.0 RC shipping PIP-460 |
| 2 | [Moonpool supervised-loop coverage](#2-moonpool-supervised-loop-coverage) | ⚡ (TLS hunk landed; supervised reconnect lines remain) |
| 3 | [Golden trace: cryptoFailureAction matrix](#3-golden-trace-cryptofailureaction-matrix) | 🔗 blocked on porting the PIP-4 crypto bridge to moonpool |
| 4 | [Differential runner: Send-bound spawn restructure](#4-differential-runner-send-bound-spawn-restructure) | 🔗 blocked on upstream moonpool `TaskProvider` |
| 5 | [Re-pin moonpool off git `branch = "main"`](#5-re-pin-moonpool-off-git-branch-main) | ⏳ blocked on a moonpool crates.io release carrying [PR #113](https://github.com/PierreZ/moonpool/pull/113) |
| 6 | [Moonpool transport `read_into` scratch allocation](#6-moonpool-transport-read_into-scratch-allocation) | 🟡 deferred (not load-bearing) |

---

## 1. PIP-460 scalable-topics e2e

**Gap.** The PIP-460 scalable-topics surface scaffold has landed across
proto / façade / both engines / CLI with the binding 4-layer in-process
tests (proto unit + tokio + moonpool 1:1 + differential + golden trace),
behind `feature = "scalable-topics"` (default off,
[ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md)). The
**e2e** test (`crates/magnetar/tests/e2e_scalable_topic.rs`) is
`#[ignore]`'d behind `feature = "e2e,scalable-topics"` with three named
tests that cannot run today — no broker ships PIP-460.

**Why it stays open.** Upstream PIP-460 is `Draft`, targeting Pulsar 5.0
LTS with phased rollout. The wire surface is hand-encoded in
`crates/magnetar-proto/src/pb/scalable_topics.rs` until a real RC ships.
Does **not** block the v0.2.0 release-cut.

**`/goal` (once a Pulsar 5.0 RC ships PIP-460).**

```text
/goal flesh out the PIP-460 e2e per docs/follow-ups.md §1 once upstream cuts a Pulsar 5.0 RC carrying PIP-460. First, as a dedicated commit per ADR-0026 §D4, run `cargo run -p xtask -- vendor-proto --rev <pulsar-5.0-rc-sha>` to replace the hand-encoded crates/magnetar-proto/src/pb/scalable_topics.rs module and reconcile field numbers against the vendored proto. Then implement the bodies of the three `#[ignore]`'d tests in crates/magnetar/tests/e2e_scalable_topic.rs against a real broker spawned via testcontainers-rs (gated `feature = "e2e,scalable-topics"`). Validation chain per CLAUDE.md.
```

---

## 2. Moonpool supervised-loop coverage

**Gap.** The moonpool transport TLS hunk and four end-to-end TLS tests
landed (`crates/magnetar-runtime-moonpool/tests/tls_transport_coverage.rs`,
1:1 tokio mirrors). The supervised reconnect loop in
`crates/magnetar-runtime-moonpool/src/driver.rs::supervised_driver_loop`
(anti-thrash cooldown, multi-attempt redial) still carries uncovered
lines.

**Why it stays open.** Closing them needs a multi-cycle peer-drop fixture
(drop, accept, drop, …) — mechanically straightforward but descoped from
the TLS-coverage PR. Pick it up if the `check-sim-coverage` diff gate
flags those lines on a future change.

**`/goal`.**

```text
/goal close the moonpool supervised-loop coverage gap per docs/follow-ups.md §2. Add a multi-cycle peer-drop fixture to crates/magnetar-runtime-moonpool/tests/ (drop → accept → drop → accept) that exercises supervised_driver_loop's anti-thrash cooldown + multi-attempt redial paths, with a paired tokio mirror to keep `xtask check-runtime-test-parity` balanced. Validation chain per CLAUDE.md, including `cargo run -p xtask -- check-sim-coverage` on the diff.
```

---

## 3. Golden trace: cryptoFailureAction matrix

**Gap.** The differential golden-trace catalog covers round-trip, batch,
nack-redelivery, seek variants, many-publishes, lookup-before-open, and
the full transactional lifecycle (new/commit, new/abort, send-ack/commit,
send-ack/abort). Missing: the **`cryptoFailureAction` matrix** (~240 LOC)
— assert each `CryptoFailureAction` arm (Fail / Discard / Consume) at the
consumer surface when a payload carries intentionally-corrupt ciphertext.

**Why it stays open.** Blocked on porting the PIP-4 message-crypto bridge
(currently `magnetar-messagecrypto` + `magnetar-runtime-tokio`) to the
moonpool runtime — the differential equivalence claim needs both engines
to drive decryption.

**`/goal` (post crypto-bridge port).**

```text
/goal add the cryptoFailureAction matrix golden trace per docs/follow-ups.md §3 — DEPENDS on porting the PIP-4 message crypto bridge to the moonpool runtime first (moonpool MessageEncryptor/Decryptor). Once those are in place, extend the scripted broker to deliver a payload with intentionally-corrupt ciphertext and assert each `CryptoFailureAction` arm (Fail / Discard / Consume) at the consumer surface. Golden trace at crates/magnetar-differential/tests/golden/crypto_failure_action.json. Validation chain per CLAUDE.md.
```

---

## 4. Differential runner: Send-bound spawn restructure

**Gap.** The differential moonpool runner's driver task is `spawn_local`'d
into a [`tokio::task::LocalSet`](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html)
because [`moonpool_core::TokioProviders`]'s `TaskProvider` spawns via
`tokio::task::Builder::new().spawn_local(...)`. While the outer test task
is parked on `consumer.receive()`, the `spawn_local`'d driver only runs
when the LocalSet's `run_until` is polled, so
[`crates/magnetar-differential/src/runner_moonpool.rs`](../crates/magnetar-differential/src/runner_moonpool.rs)
keeps a 25 ms `Kicker` pulsing `driver_waker.notify_one()` to bridge the
pump gap. Correct, just ugly.

**Why it stays open.** Investigated and structurally blocked in-tree: the
driver task is spawned **inside**
`magnetar_runtime_moonpool::Client::connect_plain` via the engine's
`TaskProvider`, which hardcodes `spawn_local`. `tokio::spawn` requires
`Send`; moonpool's `TaskProvider` is not `Send`-bound, so a drop-in
`tokio::spawn` provider is impossible without an upstream change. Two real
paths: (1) upstream moonpool adds a `Send`-bound spawn entry point (could
ride the same window as the now-merged
[PR #113](https://github.com/PierreZ/moonpool/pull/113)); or (2) duplicate
the engine's driver-spawn wiring in the runner (brittle). Until then the
`Kicker` workaround stays.

**`/goal` (post-upstream).**

```text
/goal restructure the differential moonpool runner per docs/follow-ups.md §4 ONCE the upstream moonpool TaskProvider gains a Send-bound spawn entry point. When it lands: (1) construct a custom Providers type in crates/magnetar-differential/src/runner_moonpool.rs that uses the Send-bound provider for Task and reuses TokioNetworkProvider / TokioTimeProvider / TokioRandomProvider / TokioStorageProvider for the rest; (2) drop the LocalSet wrapper in `pub async fn run(...)` — `local.run_until(run_inner(...))` becomes `run_inner(...).await`; (3) delete the Kicker struct + 25 ms pulse loop; (4) update the module doc comment to document the trade-off; (5) run golden_traces, verify no regression. Validation chain per CLAUDE.md.
```

---

## 5. Re-pin moonpool off git `branch = "main"`

**Gap.** `Cargo.toml`'s `[workspace.dependencies]` tracks `moonpool-core`
/ `moonpool-sim` on `{ version = "0.6.0", git = "…", branch = "main" }` to
consume the futures-io `TcpStream` + segment-granular `write_vectored`
change ahead of a crates.io release. This is a **documented, time-boxed
exception** to ADR-0036's exact-pin reproducibility discipline
([ADR-0043](../specs/adr/0043-temporary-floating-moonpool-git-dep.md)).
While it stands, `cargo update -p moonpool-core` can advance the rev to an
arbitrary later `main` commit; `Cargo.lock`'s concrete rev is the only
reproducibility anchor. (The `version = "0.6.0"` constraint trips
resolution if `main` crosses to 0.7, surfacing the trigger automatically.)

**Why it stays open.** Blocked on upstream cutting a **moonpool crates.io
release that contains [PR #113](https://github.com/PierreZ/moonpool/pull/113)**.
The last published release is `0.6.0`, which predates both the futures-io
migration and the vectored entry.

**`/goal` (post-release).**

```text
/goal re-pin moonpool off the git `branch = "main"` floating dependency per docs/follow-ups.md §5, once a moonpool crates.io release ships PR #113 (futures-io `NetworkProvider::TcpStream` + `SimTcpStream::poll_write_vectored`). In Cargo.toml `[workspace.dependencies]`, replace the two `{ version = "0.6.0", git = "https://github.com/PierreZ/moonpool", branch = "main" }` entries for `moonpool-core` / `moonpool-sim` with exact `=x.y.z` version pins matching the release that carries PR #113. Run `cargo update -p moonpool-core -p moonpool-sim` to refresh Cargo.lock to the released artefact. Confirm the transport still compiles against the `futures::io` ext traits (the release keeps the same surface). Remove the `[sources].allow-git` entry in deny.toml. Flip specs/adr/0043-temporary-floating-moonpool-git-dep.md Status to `Superseded by ADR-NNNN` and write the re-pin ADR (restores ADR-0036 exact-pin in full); flip the ADR-0036 amendment pointer + index status accordingly; update specs/README.md index. Update docs/simulation-patterns.md and any other version statement. Validation chain per CLAUDE.md (incl. `cargo deny check` — the release re-enables the version/advisory gates the git dep bypassed).
```

---

## 6. Moonpool transport `read_into` scratch allocation

**Gap.** `crates/magnetar-runtime-moonpool/src/transport.rs::read_into`
(the `futures::io` replacement for the vanished tokio `read_buf`)
heap-allocates a fresh `TLS_WIRE_BUFFER` (16 KiB) scratch on every read,
then copies into the caller's `BytesMut`. The old tokio `read_buf` read
in-place into the buffer's spare capacity (no extra alloc + copy).

**Why it stays open / deferred.** Not load-bearing: the moonpool engine is
the deterministic-simulation path, not the production hot path (the tokio
engine is). The heap scratch was a deliberate choice to keep the returned
future small (a 16 KiB stack array tripped clippy's `large_futures`). A
future optimization could carry a reusable scratch buffer on the
`Transport` and read into spare capacity, but it is pure throughput polish
with no correctness or behavioural impact.

---

## Notes on this file

Items move from this file to `git log` when their commit lands. The
expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** +
   (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the
   post-implementation reference); partially-landed items are trimmed to
   their remaining residual.

All remaining items carry either a `/goal …` block ready to dispatch, an
explicit external blocker (upstream moonpool / Pulsar release), or are
explicitly deferred as non-load-bearing. The only fully-external blockers
are the PIP-460 e2e ([§1](#1-pip-460-scalable-topics-e2e)) and the moonpool
re-pin ([§5](#5-re-pin-moonpool-off-git-branch-main)), both pending an
upstream release.
