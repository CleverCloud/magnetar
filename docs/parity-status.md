# Magnetar — Java Parity Status Snapshot

**Generated**: 2026-05-22 (post v0.1.0 finish-line wave)
**HEAD**: 17 commits ahead of `origin/main` — landed via the `/ask --with-codex` plan dated 2026-05-21 + the multi-agent execution wave on 2026-05-22.
**Total LOC across crates**: ~22,500 (driver + facade + proto, post-M5b) + ~3,800 (e2e suite — 14 files).

The authoritative parity matrix lives in
[`README.md#java-client-parity-matrix`](../README.md). This snapshot is a
periodic narrative of what has *recently* landed and what genuinely remains.

## Crate sizes (LOC, approximate)

| Crate | Lines | Role |
|---|---|---|
| magnetar-proto | ~7,900 | Sans-io state machine + protobuf wire types + trackers + topic watcher |
| magnetar-runtime-tokio | ~4,500 | Production engine (driver loop, producer/consumer/reader/table-view/partitioned/multi-topics/pattern façades, DNS resolver, TLS, OAuth2, cluster failover) |
| magnetar-runtime-moonpool | ~2,800 | Deterministic-simulation engine (M1–M4 landed: engine + client + producer + consumer) |
| magnetar | ~6,400 | High-level façade — `PulsarClient`, builders, typed schemas, partitioned, multi-topics, pattern, table view, crypto bridge |
| magnetar-admin | ~700 | REST admin client |
| magnetar-auth-oauth2 | ~600 | `ClientCredentialsFlow` + token caching (see [ADR-0014](../specs/adr/0014-oauth2-client-credentials-caching.md)) |
| magnetar-auth-sasl | ~300 | SASL/Kerberos scaffold (pre-alpha) |
| magnetar-auth-athenz | ~300 | Athenz scaffold (pre-alpha) |
| magnetar-messagecrypto | ~500 | PIP-4 AES-GCM bridge |
| magnetar-cli | ~250 | `magnetar` CLI |
| magnetar-fakes | ~400 | In-process broker stub for tests |
| xtask | ~250 | Workspace automation (`check-no-channels`, `check-no-io-deps`, `check-no-internal-clock`, `codegen`) |

## Recently landed (since 2026-05-22 finish-line wave — 17 commits)

Order is reverse-chronological. Each item below maps to a commit on
`main` (currently ahead of `origin/main` pending push gate (i)).

### v0.1.0 finish-line — Phase 1 (parity polish)

- **ADR-0019 — engine scope for v0.1.0 + moonpool parity train.** Clarifies
  ADR-0010 ("full Java parity") as *tokio-engine satisfied*; moonpool
  parity is the M5–M8 follow-up train. Public API: `PulsarClient<E: Engine
  = TokioEngine>` per gate (e) Option A. Commit `1c9bb5d`.
- **`MemoryLimitPolicy::ProducerBlock` (ADR-0020).** Sans-io Waker slab
  on the runtime `ConnectionShared`; `SendFut::Reserving` retries the CAS
  via `try_reserve_memory_or_register` and dispatches via
  `release_memory`-fan-out. README `:613` flipped. Commits `13842b7`,
  `81a7df0`, `458224a`.
- **README parity-matrix cleanups (Phase 1.2).** `Producer::stats`
  rolling-windows row reconciled; "Open structural gaps" section
  updated to point at the moonpool parity train, SASL/Athenz/
  AutoProduceBytesSchema deferrals, and v0.2.0 surface. Commit
  `8ecdbeb`.

### Phase 2 — moonpool engine parity train (M5)

- **Moonpool DnsResolver + transport scaffold (M5a).** `DnsResolver`
  trait + `StaticDnsResolver` + `arc_dns_resolver` helper, wired into
  `Transport::connect_plain`. Commit `b3752c7`.
- **Moonpool supervised reconnect Stage 2 + 3 (M5b).** `spawn_supervised`
  driver loop with backoff via `moonpool_core::TimeProvider`,
  `Connection::reset` integration, `rebuild_producers` +
  `rebuild_consumers` after handshake. Commit inside M5b squash.
- **Moonpool driver-TLS finish, memory_limit accounting,
  ServiceUrlProvider + `ControlledClusterFailover`, PIP-188
  `TOPIC_MIGRATED` → reset+reconnect.** All five remaining M5 surfaces
  landed via M5b squash. `AutoClusterFailover` deferred — see
  `docs/m5b-deferrals.md`.

### Phase 3 — e2e expansion (Batches A..E + A2 + A3 + C2)

- **e2e_crypto.rs** — PIP-4 + Fail/Discard/Consume action coverage + PIP-37
  chunking+encryption cross. Commit `ee13d29`.
- **e2e_pattern_auto_reconcile.rs** — PatternConsumer background ticker
  rediscovers topics. Commit `6b645ff`.
- **e2e_oauth2.rs** — `ClientCredentialsFlow` round-trip + token cache +
  refresh-on-expiry via injectable `Clock`. Commit `bb80b6d`.
- **e2e_seek_per_partition.rs + e2e_memory_limit.rs +
  e2e_force_unsubscribe.rs (Batch A).** Commit `5b6e859`.
- **e2e_dns_resolver.rs + Producer/Consumer::record_rate_window helpers
  (Batch A2).** Commit `9f4db4e`.
- **e2e_rolling_stats.rs + e2e_schemas_extended.rs (Batch A3).** Commit
  `6aadbaf`.
- **e2e_reconnect.rs + e2e_cluster_failover.rs (Batch C2).** Stage 2/3
  supervised reconnect under broker stop/start; PIP-121 manual swap with
  two broker containers. Commit `dc5a76f`.

### Phase 4 — CI hardening

- **`cargo xtask codegen --check` + MSRV-1.85 job + dependabot.** Commit
  `6b387b2`. `/loop` CI-monitor note in `docs/ci-monitor-loop.md`.

### Phase 0 — worktree triage + WIP terminations

- **38 → 1 worktree.** Bulk-dropped 28 subset-of-main worktrees; 5 WIP
  terminations (W2/W3/W4/W5/W7/W8) with reports in `docs/wip-W*-report.md`.
  Commits `8698961`, `2191176`, `8821ab0`, `8ecdbeb`.

## Previously landed (pre-2026-05-22 — 50+ commits)

Order is reverse-chronological. Each item below maps to either an ADR
or a row flipped in the README parity matrix.

### Parity-matrix flips → ✅

- **PIP-121 cluster failover** — `ServiceUrlProvider` trait,
  `StaticServiceUrlProvider`, `ControlledClusterFailover` (manual swap),
  and `AutoClusterFailover` with a `HealthProbe` trait + background
  prober. Plumbed through the supervised reconnect path so a swap
  triggers re-handshake on the new URL.
  See [ADR-0016](../specs/adr/0016-pip-121-cluster-failover.md).
- **PIP-188 `TOPIC_MIGRATED` → reconnect-on-migrate** — driver now
  surfaces a `TopicMigrated` event that returns `ClientError` to trigger
  the supervised reset + reconnect with the new URL.
  See [ADR-0018](../specs/adr/0018-pip-188-reconnect-on-migrate.md).
- **`memory_limit` runtime accounting** — `ClientBuilder::memory_limit`
  + `MemoryLimitPolicy`; producer reserves bytes via atomic CAS on send,
  releases on completion via `SendFut::Drop`.
  See [ADR-0017](../specs/adr/0017-memory-limit-atomic-reservation.md).
- **DNS resolver injection** — `DnsResolver` trait +
  `TokioDnsResolver`; `Transport::connect_with_resolver` helper;
  `ClientBuilder::dns_resolver`. Java parity for `dnsResolver`.
  See [ADR-0015](../specs/adr/0015-dns-resolver-injection.md).
- **OAuth2 `ClientCredentialsFlow` with token caching** — token TTL
  awareness + `Clock` trait for testable expiry; 8 unit tests.
  See [ADR-0014](../specs/adr/0014-oauth2-client-credentials-caching.md).
- **TLS knobs** — `tls_allow_insecure_connection` (blanket) and
  `tls_hostname_verification_enable` (chain-on / hostname-off via a
  `WebPkiServerVerifier` wrapper).
- **PIP-4 `cryptoFailureAction`** — `Fail` / `Discard` / `Consume`
  knob on `ConsumerBuilder` and `TableViewBuilder`.
- **PIP-37 `AckTimeoutRedeliveryBackoff`** — backoff propagated to
  `ConsumerBuilder.negative_ack_backoff` and used in nack tracker.
- **Auto-update background ticker** for `PatternConsumer` (topic
  rediscovery on a periodic tick) and `TableView` (partition rediscovery).
- **`hdrhistogram` p50/p99/max** — added to allow-list; per-send /
  per-receive latency histograms; `ConsumerStats` / `ProducerStats`
  surface the percentiles.
- **Rolling per-second windows** for `msgs/sec` and `bytes/sec` on
  `ConsumerStats` and `ProducerStats`.
- **`AutoConsumeSchema` broker lookup** — runtime fetches the topic
  schema on first receive (PIP-87).

### Tests

- **10 new e2e suites** (~2,400 LOC) covering Java parity end-to-end:
  - `e2e_schemas` — Bytes/String/JSON/Int32 round-trips.
  - `e2e_dlq` — DLQ + `reconsume_later`.
  - `e2e_batch_chunk` — batching + chunking (PIP-37).
  - `e2e_interceptors_ack` — interceptor SPIs + ack patterns.
  - `e2e_transactions` — commit/abort round-trips (PIP-31, gated).
  - `e2e_sub_types` — Shared / Failover / Key_Shared.
  - `e2e_partitioned_deep` — partitioned producer + consumer.
  - `e2e_compacted` — compacted topics + TableView (PIP-94).
  - `e2e_persistence` — persistent + non-persistent semantics.
  - `e2e_pulsar` — basic round-trip smoke.
- **220+ unit tests** across the workspace, including 13 ported tracker
  cases, 6 ported batch-container cases, and ~14 ported schema cases.

### CI

- `moonpool-sim` job: runs the deterministic-simulation suite on every push/PR.
- `e2e` job: spins `apachepulsar/pulsar:4.0.4` and runs the suite on every push/PR.
- `no-internal-clock` job: `cargo xtask check-no-internal-clock` greps
  `magnetar-proto` for `Instant::now()` / `SystemTime::now()` outside `#[cfg(test)]`.
- Cargo audit clear: time 0.3.45 CVE + rustls-pemfile unmaintained both resolved
  (bumped `rustls-native-certs` 0.7→0.8, swapped to `rustls-pki-types::PemObject`).

## Genuine deferred-scope items

Everything else with a `🟡` / `❌` in the README parity matrix is one of:

| Item | Status | Why deferred |
|---|---|---|
| **SASL (Kerberos)** | 🟡 pre-alpha | Crate scaffolded; full GSSAPI integration is large-scope. |
| **Athenz** | 🟡 pre-alpha | Crate scaffolded; ZTS/ZMS plumbing is large-scope. |
| **`AutoProduceBytesSchema`** | 🟡 trait surface only | Less common than `AutoConsumeSchema` (✅); deferred while consumer path is the common case. |
| **PIP-460 — Scalable topics** | ❌ | Experimental in Apache Pulsar; M9 scope. |
| **PIP-466 — V5 client surface** | ❌ | Inspired by, not adopted verbatim. M9 evaluation. |
| **PIP-180 — Shadow topic** | ❌ | M9 scope. |
| **PIP-415 — `getMessageIdByIndex`** | ❌ | Blocked on vendored proto bump. |
| **PIP-33 — Replicated subscriptions** | ❌ | M9 scope. |

These are tracked in [`docs/implementation-plan.md`](implementation-plan.md)
under "M9 — beyond v0.1.0 parity"; they are *not* required for v0.1.0
under [ADR-0010](../specs/adr/0010-v0-1-full-java-parity.md).

## Moonpool parity train (M5 → M8)

Per [ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md),
the v0.1.0 Java parity matrix in `README.md` is satisfied **by the tokio
engine**. The moonpool engine reaches feature parity with tokio in a
follow-up train; this section tracks the gap.

| Surface | tokio | moonpool | Milestone |
|---|---|---|---|
| Engine driver loop + transport scaffold | ✅ | ✅ (M1) | – |
| Client (lookup + partitioned-metadata + topic-watch) | ✅ | ✅ (M2) | – |
| Producer façade (send / flush / close) | ✅ | ✅ (M3) | – |
| Consumer façade (subscribe / receive / ack) | ✅ | ✅ (M4) | – |
| Supervised reconnect (Stage 2 / Stage 3) | ✅ | ❌ | M5 |
| DNS resolver injection (ADR-0015) | ✅ | ❌ | M5 |
| Driver-level TLS (rustls-over-bytepipe) | ✅ | partial | M5 |
| `memory_limit` atomic-CAS accounting (ADR-0017) | ✅ | ❌ | M5 |
| `ServiceUrlProvider` plumbing (ADR-0016 / PIP-121) | ✅ | ❌ | M5 |
| PIP-188 `TOPIC_MIGRATED` reconnect-on-migrate (ADR-0018) | ✅ | ❌ | M5 |
| Generic `PulsarClient<E: Engine>` (gate (e)) | ✅ | ✅ (M6) | – |
| Partitioned producer / consumer (façade) | ✅ | ❌ | M7–M8 |
| MultiTopicsConsumer (façade) | ✅ | ❌ | M7–M8 |
| PatternConsumer (façade) | ✅ | ❌ | M7–M8 |
| Reader (façade) | ✅ | ❌ | M7–M8 |
| TableView (façade) | ✅ | ❌ | M7–M8 |
| Transactions (PIP-31 façade) | ✅ | ❌ | M7–M8 |
| Typed schemas (façade) | ✅ | ❌ | M7–M8 |
| Deterministic chaos pack (mid-handshake partition, frame reorder, virtual-clock timeouts, OAuth refresh edges, PIP-121 oscillation, PIP-188 migrate-then-migrate-again) | n/a | planned | M7 |
| tokio ↔ moonpool differential equivalence harness | n/a | planned | M8 |

M6 landed the generic `PulsarClient<E: Engine = TokioEngine>` façade
(2026-05-22, [ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md)
gate (e), "Option A"). The moonpool branch exposes
`PulsarClient<MoonpoolEngine<P>>` with the shared connection state +
driver-handle wrapper but does **not** yet carry the partitioned /
multi-topics / pattern / reader / table-view / typed-schema /
transactions surface — those stay bound to
`PulsarClient<TokioEngine>` and lift across as part of M7–M8.

Until then, callers that reach for a tokio-only method on the
moonpool engine get a trait-bound compile error, not a silent
fallback — see ADR-0019 §Consequences.

## Constraints recap

- **No channels**: never use `tokio::sync::mpsc / broadcast / oneshot / watch`,
  `crossbeam-channel`, `flume`, `async-channel`. Use
  `Arc<parking_lot::Mutex<...>>` + `tokio::sync::Notify` + per-future Waker slabs.
  See [ADR-0003](../specs/adr/0003-no-channels-rule.md).
- **Sans-io clock injection**: every `magnetar-proto::Connection` entry takes
  `now: Instant` + `wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>`.
  See [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md).
- **Commits**: GPG-signed via `git commit -s -S` (enforced by hook).
- **Branches**: `feat/<scope>`, `fix/<scope>`, etc.
- **Worktree-first**: `wt switch --create feat/<scope> -y`, work, `wt merge -y`.
  See [ADR-0013](../specs/adr/0013-worktree-first-development.md).
- **No Claude attribution** on commits / PRs / MRs.
  See [ADR-0012](../specs/adr/0012-no-claude-attribution.md).
- **Conventional commits**: `<type>(<scope>): <subject>`.
- **Validation chain (every commit)**:
  ```
  cargo build --all-features
  cargo test --all-features --workspace
  cargo clippy --all-features --workspace -- -D warnings
  cargo +nightly fmt --all
  RUSTDOCFLAGS="-D warnings --cfg tokio_unstable" \
    cargo doc --no-deps --all-features --workspace --locked
  cargo deny check
  cargo xtask check-no-channels
  cargo xtask check-no-io-deps
  cargo xtask check-no-internal-clock
  cargo xtask codegen --check
  ```
