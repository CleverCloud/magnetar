# Magnetar — Java Parity Status Snapshot

**Generated**: 2026-05-21 (post parity-matrix resync)
**HEAD**: `810278f docs(parity-matrix): resync rows clobbered by stale-base merges`
**Total LOC across crates**: ~21,000 (driver+facade+proto) + ~2,400 (e2e suite)

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

## Recently landed (since last snapshot — 50 commits)

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
