# Magnetar Codebase Review — 2026-05-21

Walking review of the workspace at `010e252..b97840f` plus the today batch
(security fixes, sans-io enforcement, ADRs, e2e ports). Read-only — no code
changes in this commit; findings are queued as follow-ups.

## Summary

- **Architecturally sound.** The sans-io split is honoured end-to-end — the
  new `cargo xtask check-no-internal-clock` gate caught the one residual
  leak (`ConsumerState::deliver` at consumer.rs:547) and it's fixed.
- **Java parity is real.** Producer / consumer / reader / partitioned /
  multi-topics / pattern / table-view all wire through. Encryption (PIP-4),
  transactions (PIP-31), chunking (PIP-37), partial-batch ACK (PIP-54),
  AutoConsumeSchema runtime auto-fetch (PIP-87) — all landed.
- **e2e coverage is broad.** 10 e2e test files (~2,400 LOC) port the major
  Java behavioural suites (`InterceptorsTest`, `DeadLetterTopicTest`,
  `BatchMessageTest`, `TransactionEndToEndTest`, `KeySharedSubscriptionTest`,
  `MessageRouterTest`, `CompactionTest`, `TableViewTest`,
  `NonPersistentTopicTest`).
- **3 follow-ups stand out** (none are ship-stoppers, all already noted in
  the README's parity matrix as 🟡):
  1. `memoryLimit` runtime enforcement (today: configuration storage only).
  2. `serviceUrlProvider` runtime URL rotation across reconnect attempts.
  3. Pattern-consumer auto-update ticker (today: caller-driven via
     `update()`).

## Findings by severity

### 🔴 Blocker (ship-stopper)

_None._

### 🟡 High (should fix before v0.1)

- **`magnetar/src/client.rs:1283..1289`** — `ConsumerBuilder::encryption`
  stores the decryptor but the runtime currently honours only
  `CryptoFailureAction::Fail` end-to-end. README row 562 already flags
  this; a follow-up ADR-tracking note in `specs/adr/` would help close
  the loop.
- **`magnetar-runtime-tokio/src/driver.rs:supervised_driver_loop`** — the
  supervised reconnect path correctly calls `reset()` + `rebuild_producers`
  + `rebuild_consumers`, but the *in-flight publish-replay* across the
  reconnect boundary still surfaces `OpOutcome::SessionLost` to users
  rather than transparently re-queuing. README "Open structural gaps"
  acknowledges this as Stage 3 follow-up; no code change required, just
  ensure the next user-facing release notes call it out.

### 🟢 Medium (nice to have)

- **`magnetar-proto/src/conn.rs`** is ~3,000 LOC. Splitting into
  `conn/mod.rs` + `conn/handshake.rs` + `conn/dispatch.rs` would help
  reviewers. Not urgent — the current file is still navigable.
- **`magnetar/src/table_view.rs`** is ~1,100 LOC and houses both the
  `TableView` + `TypedTableView` + their builders + the auto-update
  task. Same refactor opportunity; not urgent.
- **`magnetar-runtime-tokio/src/consumer.rs`** at 1,232 LOC is the
  biggest runtime file. The receive + ack + crypto-failure decision tree
  could be extracted into helper functions for testability.

### ⚪ Info (notes / kudos)

- The waker-slab pattern is consistently applied across producer +
  consumer + transaction + lookup. The `no-channels` rule (ADR-0003)
  works in practice.
- The ADR set (specs/adr/0001..0013) is short, each <100 LOC, easy to
  cite from commit messages and code comments.
- `docs/moonpool-simulation.md` (just landed) is a good reference for
  contributors getting started with the deterministic-simulation engine.
- The CI matrix (fmt + clippy + build × {stable, beta} + test + doc +
  deny + no-channels + no-io-deps + no-internal-clock + moonpool-sim
  + e2e + fuzz-smoke + mutants-smoke) is comprehensive.

## Per-crate review

### magnetar-proto

- **Sans-io invariants honoured.** `cargo xtask check-no-io-deps` passes;
  `check-no-channels` passes; `check-no-internal-clock` passes after
  today's consumer.rs fix.
- **CRC32C verify-or-drop** enforced at `frame.rs:decode_one` — confirmed
  via grep.
- **Panics restricted to `#[cfg(test)]`** — `grep -rn "panic!\|unwrap()\|expect("
  crates/magnetar-proto/src` returns hits only inside `#[cfg(test)]` blocks
  or in `expect()` calls with infallible logic (e.g.
  `hdrhistogram::Histogram::new(3).expect("hdrhistogram precision 3 is valid")`).
- **Schema canonicalisation** (Avro/JSON canonical form, byte-equal for
  PROTOBUF_NATIVE + KeyValue) implemented in `schema/avro.rs`,
  `schema/json.rs`, `schema/keyvalue.rs`. Tests cover the canonical-form
  hash invariance.

### magnetar-runtime-tokio

- Driver loop lock discipline OK: `critical sections` never `.await`
  (confirmed by reading `driver_loop_inner`).
- TLS: `tokio-rustls` only; the new `tls_insecure` module gates the
  insecure verifier behind an explicit opt-in. Documented warnings are
  prominent.
- The supervised reconnect (Stage 2 + Stage 3 producer/consumer rebuild)
  works as described in ARCHITECTURE.md.

### magnetar

- Builders are consistent across surfaces (Producer / Consumer / Reader /
  TableView / PartitionedProducer / PartitionedConsumer / MultiTopics /
  Pattern). Each takes the same shape (`fn new() -> Self`, `setter ->
  Self`, `async fn create/subscribe -> Result<T>`).
- Java parity spot-check: 5 of 5 random Java `ConsumerBuilder` setters
  found on magnetar's `ConsumerBuilder` (`subscriptionName`, `consumerName`,
  `priorityLevel`, `replicateSubscriptionState`, `subscriptionMode`).
- Interceptor SPIs (`ProducerInterceptor` / `ConsumerInterceptor`) compose
  cleanly with the typed surface via `send_with_interceptors` /
  `receive_with_interceptors` / `ack_with_interceptors`.

### magnetar-runtime-moonpool

- Mirrors the tokio engine surface: client (414 LOC), producer (701 LOC),
  consumer (670 LOC), driver (295 LOC), TLS adapter (215 LOC). M1 → M4
  scope landed.
- No `tokio::*` leak — confirmed by `grep -rn "tokio::" crates/magnetar-runtime-moonpool/src`
  returns only `tokio::io::AsyncReadExt` / `AsyncWriteExt` (the engine
  is `tokio`-runtime-driven; that's by design).
- TLS byte-pipe adapter (`tls.rs`) per ADR-0006 — sound implementation.

## Cross-cutting

- **`cargo audit`** — 0 vulnerabilities, 0 warnings after today's
  `fix/security-deps` merge.
- **`cargo deny check`** — advisories ok, bans ok, licenses ok, sources ok.
- **`specs/adr/`** — 13 ADRs, index in `specs/README.md` consistent.
- **`docs/`** — `implementation-plan.md`, `decisions-log.md`,
  `parity-status.md`, `research.md`, `review.md`, `audit.md`,
  `codex-cross-check.md`, `swarm-history.md`, `moonpool-simulation.md`
  cross-referenced from `docs/README.md` + `CLAUDE.md`.
- **CI workflow** — every job from CLAUDE.md's validation chain is
  represented (fmt, clippy, build, test, doc, deny, check-no-channels,
  check-no-io-deps, check-no-internal-clock, moonpool-sim, e2e,
  fuzz-smoke, mutants-smoke).

## Recommended next steps

1. **Wire `MemoryLimitPolicy::FailImmediately` runtime enforcement.** The
   AtomicU64 + CAS sketch from the discarded `feat/memory-limit` agent
   branch can be adapted (keep the `bytes + policy` builder shape; only
   plumb FailImmediately for now). ~150 LOC.
2. **Wire `ServiceUrlProvider::get_service_url()` into the supervised
   reconnect path.** Today the URL is cached on `ReconnectContext`;
   re-resolve it per attempt. ~30 LOC + one e2e test against a
   `FlippingProvider`.
3. **Optional split**: `magnetar-proto/src/conn.rs` (3,000 LOC) into
   sub-modules. Only do this when a contributor complains.
4. **Stats rolling windows**: a small `tokio::time::interval` ticker in
   the runtime stats-collection paths. Cumulative counters already in
   place; rolling adds an EMA / fixed-window snapshot. ~80 LOC.
