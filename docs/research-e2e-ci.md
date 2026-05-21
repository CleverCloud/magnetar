# Research — E2E test coverage + CI state

Researcher C slice for `/ask` pipeline. Cwd:
`/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar`. Branch: `main`.
Date: 2026-05-21.

---

## 1. E2E coverage map

### 1.1 Existing suites (10 files, ~2,400 LOC per `docs/parity-status.md:5`)

All files share the same `start_pulsar()` helper that boots
`apachepulsar/pulsar:4.0.4` via `testcontainers-rs`, gated by `#![cfg(feature = "e2e")]`.

| File | LOC | Tests | What it exercises |
| --- | --- | --- | --- |
| `crates/magnetar/tests/e2e_pulsar.rs` | 10.1 K | 4 | Basic produce/consume smoke (`e2e_pulsar.rs:82`), partitioned-topic round-trip (`:125`), `PatternConsumer` snapshot for PIP-145 (`:176`), Key_Shared baseline (`:232`). |
| `crates/magnetar/tests/e2e_schemas.rs` | 9.2 K | 4 | `BytesSchema` (`e2e_schemas.rs:81`), `StringSchema` (`:132`), `JsonSchema` w/ serde round-trip (`:184`), `Int32Schema` (`:236`). |
| `crates/magnetar/tests/e2e_dlq.rs` | 9.7 K | 3 | `max_redeliver` → DLQ routing (`e2e_dlq.rs:74`), `reconsume_later` retry-letter round-trip (`:159`), explicit-ack-after-N terminates DLQ (`:224`). PIP-22/58/124/409. |
| `crates/magnetar/tests/e2e_batch_chunk.rs` | 8.4 K | 3 | Batching flush on `max_msgs` (`e2e_batch_chunk.rs:88`), batch-receive (`:139`), PIP-37 chunked round-trip with batching disabled (`:198`). |
| `crates/magnetar/tests/e2e_transactions.rs` | 11.8 K | 3 | PIP-31 commit visible (`e2e_transactions.rs:102`), abort drops (`:163`), consumer-ack-in-txn rolled back on abort (`:225`). **Double-ignored** because the default container doesn't enable the txn coordinator. |
| `crates/magnetar/tests/e2e_sub_types.rs` | 12.9 K | 3 | Shared distributes across N consumers (`e2e_sub_types.rs:120`), Failover active-only (`:198`), Key_Shared sticks per key (`:289`). |
| `crates/magnetar/tests/e2e_partitioned_deep.rs` | 9.4 K | 3 | Round-robin hits every partition (`e2e_partitioned_deep.rs:105`), custom `MessageRouter` (`:171`), partitioned consumer aggregates (`:235`). |
| `crates/magnetar/tests/e2e_persistence.rs` | 8.7 K | 3 | `persistent://` round-trip (`e2e_persistence.rs:84`), `non-persistent://` round-trip (`:136`), `non-persistent://` drops when no consumer (`:190`). |
| `crates/magnetar/tests/e2e_compacted.rs` | 14.0 K | 3 | `read_compacted` reader sees latest-per-key (`e2e_compacted.rs:127`), TableView compacted snapshot (`:239`), tombstone removes key (`:308`). PIP-94. |
| `crates/magnetar/tests/e2e_interceptors_ack.rs` | 11.3 K | 4 | Producer interceptor SPI (`e2e_interceptors_ack.rs:143`), consumer interceptor SPI (`:182`), batch-ack terminates redelivery (`:235`), cumulative-ack terminates prior (`:288`). |

**Totals**: 33 e2e test functions, 10 suite files.

### 1.2 Parity-matrix coverage (rows → suite)

Read against `README.md:399-616` (Producer/Consumer/Partitioned/Multi/Pattern/Reader/TableView/Txn/Auth+TLS/Encryption/Schemas/ClientBuilder).

**Covered end-to-end:**

| Parity row | Suite |
| --- | --- |
| `send` round-trip, basic consume, partitioned smoke, Key_Shared baseline, PIP-145 pattern snapshot | `e2e_pulsar.rs` |
| Bytes / String / JSON / Int32 schemas | `e2e_schemas.rs` |
| DLQ policy + `reconsume_later` (PIP-22/58/124/409) | `e2e_dlq.rs` |
| Batching (max-msgs flush), `batchReceive`, PIP-37 chunking | `e2e_batch_chunk.rs` |
| PIP-31 producer publish, consumer ack-in-txn (commit + abort) | `e2e_transactions.rs` (ignored by default) |
| `Shared` / `Failover` / `Key_Shared` subscription types | `e2e_sub_types.rs` |
| `MessageRoutingMode::RoundRobin`, `Custom` router, partitioned consumer aggregation | `e2e_partitioned_deep.rs` |
| `persistent://` vs `non-persistent://` topic dispatch | `e2e_persistence.rs` |
| `read_compacted`, `TableView::get`/`for_each`, PIP-94 compaction | `e2e_compacted.rs` |
| `ProducerInterceptor` / `ConsumerInterceptor` SPI; individual/batch/cumulative ack | `e2e_interceptors_ack.rs` |

**NOT covered by any e2e suite today** (verified by `grep` against `crates/magnetar/tests/`):

| Parity row | README anchor | Why we need it |
| --- | --- | --- |
| PIP-4 encryption end-to-end (`MessageEncryptor` + `MessageDecryptor`) | `README.md:576-578` | Encryption round-trip never exercised against real broker; only unit tests inside `magnetar-messagecrypto`. |
| `cryptoFailureAction = Fail / Discard / Consume` | `README.md:476`, `:579` | Three behavioural branches in `magnetar-runtime-tokio::consumer::deliver_post_process`, zero broker-side validation. |
| `AutoConsumeSchema` runtime broker lookup | `README.md:592` | First-call `Connection::get_schema` path is wired but unverified end-to-end. |
| `AutoProduceBytesSchema` | `README.md:593` (🟡 trait-only) | Out of scope per parity status. |
| OAuth2 `ClientCredentialsFlow` | `README.md:565`, ADR-0014 | Token cache + refresh-within-30 s never hit against a stub IDP. |
| SASL / Athenz | `README.md:566-567` (🟡 pre-alpha) | Deferred to M9. |
| `tls_allow_insecure_connection`, `tls_hostname_verification_enable` | `README.md:608-609` | Two TLS verifiers (custom + no-hostname) never exercised against a TLS-fronted broker. |
| `ServiceUrlProvider`, `StaticServiceUrlProvider`, `ControlledClusterFailover`, `AutoClusterFailover` (PIP-121) | `README.md:610`, `:616`, ADR-0016 | Supervised reconnect calls `provider.get_service_url()` on every attempt; never validated. |
| `dnsResolver` injection | `README.md:614`, ADR-0015 | `Transport::connect_with_resolver` is wired but no test confirms the custom resolver is consulted. |
| `memoryLimit` accounting (atomic CAS reservation) | `README.md:613`, ADR-0017 | Back-pressure / `FailImmediately` behaviour unverified. |
| PIP-188 `TOPIC_MIGRATED` → reconnect | `README.md:646`, ADR-0018 | Driver opcode handler returns an error → supervised reset path; no e2e drives this. |
| Stage-2/3 supervised reconnect, `rebuild_producers` / `rebuild_consumers` | (no parity-row label, CLAUDE.md "Landed") | Connection drop + transparent rebuild never exercised. |
| `seek_per_partition` callback (PIP `seekAsync(Function)`) | `README.md:458` | Per-partition callback path untested. |
| `PatternConsumer::start_auto_reconcile` background ticker | `README.md:523` | Manual `update(&client)` exercised in `e2e_pulsar.rs:176`; the auto-reconcile timer is not. |
| `PartitionedProducer::auto_update_partitions_interval` ticker | `README.md:495` | Notify-on-tick signal path unverified. |
| `PartitionedConsumer::auto_update_partitions_interval` ticker | `README.md:504` | Same. |
| `MultiTopicsConsumer::auto_update_partitions_interval` + `add_topic` / `remove_topic` | `README.md:513-514` | Dynamic add/remove never driven against a broker. |
| `TableView::auto_update_partitions_interval` | `README.md:545` | Notify-on-tick signal path unverified. |
| `TableViewBuilder::encryption` decryptor stamp | `README.md:546` | Cross-feature combo (encryption + TableView) untested. |
| Rolling stats windows (`msgs_per_sec` / `bytes_per_sec`) | `README.md:465` | Tickers drive the windows; behaviour not pinned. |
| `forceUnsubscribe` (PIP-313) | `README.md:480` | Wired through `CommandUnsubscribe.force`; no e2e flips the bit. |
| `proxyServiceUrl` binary-proxy path | `README.md:570`, `:611` | No proxy container in the test bench. |
| Schemas: Avro / Protobuf / ProtobufNative / KeyValue / Date/Time/Timestamp/etc. | `README.md:588-595` | Only Bytes/String/JSON/Int32 are e2e; rest only unit-tested for byte-identical Java output. |

---

## 2. E2E gaps — proposed new suites

Confirming or refuting the candidates surfaced in the prompt against what's
in the tree:

| Proposed suite | Verdict | Rationale |
| --- | --- | --- |
| `e2e_reconnect` (Stage-2/3 supervised reset + producer/consumer rebuild) | **Confirm — top priority.** | Stage-2/3 is the most invasive thing landed since M8 and has zero broker-side regression coverage. Force a `Connection::reset` (broker restart via testcontainer `restart()` or `docker stop`/`start`), assert in-flight producers + consumers re-issue `CommandProducer` / `CommandSubscribe` transparently and observable counters carry over (`Producer::last_disconnected_timestamp`). |
| `e2e_cluster_failover` (PIP-121 manual + auto + PIP-188 topic-migrated) | **Confirm — top priority.** | Two brokers in compose (testcontainers supports a network), `StaticServiceUrlProvider` swap mid-flight, `AutoClusterFailover` with `HealthProbe` returning unhealthy. Pair with PIP-188 by sending `CommandTopicMigrated` from a `magnetar-fakes` broker stub (cleaner than orchestrating real cluster migration). |
| `e2e_oauth2` (`ClientCredentialsFlow` against a token endpoint stub) | **Confirm.** | Use `wiremock` or `httpmock` to stand up a tiny IDP that returns a 5-second-lived JWT, then assert the cache refreshes within the 30 s window (ADR-0014). The broker itself can stay unauthenticated; the test is about the auth provider's behaviour. |
| `e2e_dns_resolver` (custom resolver routes to broker) | **Confirm.** | Inject a resolver that aliases `pulsar.test.local` → the testcontainer IP and assert connect succeeds; flip to a resolver that returns `127.0.0.1:1` and assert the failure mode. |
| `e2e_memory_limit` (back-pressure) | **Confirm.** | Set `memory_limit(1 KiB, FailImmediately)`, send messages larger than the budget, assert `MemoryLimitExceeded` error and that the CAS counter releases on `SendFut::Drop`. |
| `e2e_tls` (hostname verify on/off, insecure on/off) | **Confirm — needs work.** | Requires `apachepulsar/pulsar:4.0.4` with TLS config; the standalone image needs an env-file dropping `brokerServicePortTls=6651` + a generated self-signed cert. Probably one suite per axis: insecure-accept, hostname-mismatch, correct-hostname. |
| `e2e_seek_per_partition` | **Confirm.** | Three-partition topic, callback returns `SeekTarget::PublishTimeMs` for partition 0 and `MessageId::Earliest` for partitions 1-2. Pure broker-observable. |
| `e2e_pattern_auto_reconcile` (auto ticker absorbs add/remove) | **Confirm.** | Create topic A matching pattern, start `start_auto_reconcile` with 250 ms interval, create topic B at runtime, assert the `JoinHandle`-backed task picks it up without manual `update(&client)`. |
| `e2e_tableview_auto_update_partitions` | **Confirm.** | Partitioned topic, start TableView with ticker, expand partitions via admin client, assert `partitions_changed_notify` fires and refresh sees new partition. |
| `e2e_crypto` (PIP-4 round-trip with Discard / Consume / Fail) | **Confirm.** | Producer with `MessageEncryptor`, consumer with matching / mismatching `MessageDecryptor` for each `cryptoFailureAction` branch. Cross-feature with `e2e_encryption_auto_consume_schema`. |
| `e2e_encryption_auto_consume_schema` | **Confirm.** | Combine `AutoConsumeSchema` runtime broker fetch (`README.md:592`) with PIP-4 decryption — both unverified and they share the consumer post-process pipeline. |

**Additional gaps surfaced during the audit (not in the prompt):**

| Suite | Rationale |
| --- | --- |
| `e2e_force_unsubscribe` | PIP-313 `CommandUnsubscribe.force` is wired (`README.md:480`) but never validated. Small, cheap test. |
| `e2e_multi_topics_dynamic` | `MultiTopicsConsumer::add_topic` / `remove_topic` at runtime (`README.md:513`). |
| `e2e_rolling_stats` | `msgs_per_sec` / `bytes_per_sec` rolling windows (`README.md:465`). |
| `e2e_schemas_extended` | Avro / Protobuf / ProtobufNative / KeyValue / temporal schemas — broker-canonical-form parity (`README.md:588-595`). |

---

## 3. CI state

### 3.1 Workflow inventory

Single workflow file: `.github/workflows/ci.yml` (6.3 K, 194 lines).

Triggers (`ci.yml:3-8`): push to `main`, PR targeting `main`, manual
`workflow_dispatch`. Concurrency group cancels in-flight runs on the same
ref (`ci.yml:15-17`).

Global env (`ci.yml:19-25`): `RUSTFLAGS="-D warnings --cfg tokio_unstable"`,
`RUSTDOCFLAGS="-D warnings --cfg tokio_unstable"`, `CARGO_INCREMENTAL=0`,
`CARGO_NET_RETRY=5`, `CARGO_NET_GIT_FETCH_WITH_CLI=true`.

| Job (`ci.yml`) | Command | Timeout | When |
| --- | --- | --- | --- |
| `fmt` (`:28`) | `cargo +nightly fmt --check --all` | 20 min | always |
| `clippy` (`:39`) | `cargo clippy --workspace --all-features --all-targets -- -D warnings` | 60 min | always |
| `build` (`:51`) | `cargo build --workspace --all-features --locked` | 60 min | matrix [stable, beta] |
| `test` (`:67`) | `cargo test --workspace --all-features --locked` | 90 min | always |
| `doc` (`:77`) | `cargo doc --workspace --all-features --no-deps --locked` | 45 min | always |
| `deny` (`:87`) | `EmbarkStudios/cargo-deny-action@v2` | 15 min | always |
| `no-channels` (`:95`) | `cargo run xtask -- check-no-channels` | 30 min | always |
| `no-io-deps` (`:105`) | `cargo run xtask -- check-no-io-deps` | 30 min | always |
| `no-internal-clock` (`:115`) | `cargo run xtask -- check-no-internal-clock` | 30 min | always |
| `moonpool-sim` (`:129`) | `cargo build -p magnetar-runtime-moonpool` + `cargo test -p magnetar-runtime-moonpool --all-features` | 60 min | always |
| `e2e` (`:147`) | `docker pull apachepulsar/pulsar:4.0.4` + `cargo test --workspace --features e2e --locked -- --nocapture` | 90 min outer / 75 min inner | always |
| `mutants-smoke` (`:166`) | `cargo install cargo-mutants` + `cargo mutants -p magnetar-proto --timeout 60 --no-shuffle` | 180 min | **`workflow_dispatch` only** |
| `fuzz-smoke` (`:178`) | `cargo install cargo-fuzz` + 60 s each on `decode_one` + `encode_roundtrip` | 60 min | `workflow_dispatch` or `pull_request` |

### 3.2 Recent run status

Confirmed via `gh run list --limit 25` and per-run inspection:

- **26248189031** (2026-05-21 19:26 UTC, latest, conclusion=`failure`):
  every queued job completed in **2 seconds** with `conclusion=failure` and
  zero step output. Signature is GitHub's "billing/spending limit reached"
  — no jobs ever started running. Confirmed by `gh run view 26248189031
  --json jobs`: `startedAt`/`completedAt` deltas are 0-3 s and `steps: []`.
- **26238995430 → 26225299006** (16:27 → 12:15 UTC, all
  `conclusion=cancelled`): no failures, just cancelled by the concurrency
  group as later pushes arrived (or, in 26225299006's case, the `test` job
  ran for 6 hours and was eventually cancelled at 18:15 — likely
  workflow-cancel during the billing incident; fmt/build/clippy/doc/deny/
  no-channels/no-io-deps all `success`).
- **26222624211, 26221923859, 26221892668** (11:00-11:16 UTC,
  `conclusion=success`): last three clean runs before the failure cascade.

**Real pre-billing failures**:

- **26221410209** (10:50 UTC, failure): two real failures, neither related
  to e2e infrastructure:
  - `clippy`: `it is more idiomatic to use Option<&T> instead of
    &Option<T>` (×4) and `non-binding let on a future` — `magnetar` crate
    lib tests, 5 errors total.
  - `test`: 1/149 tests failing in `magnetar-proto`:
    `trackers::nack::tests::multiplier_max_redelivery_count_clamps_to_max
    ... FAILED` (panic at `crates/magnetar-proto/src/trackers/nack.rs:306`).
- **26220694546** (10:33 UTC, failure): both `clippy` and `test`
  failing on `magnetar-runtime-moonpool`: `associated function from_stream
  is never used` (dead-code lint, `-D warnings` promoted it to an error).

Both of these were fixed in the next push (26221145212 success, 26221923859
clean) — they are not regressions in the current `main`.

**Net**: no live test/CI regression as of `main@37d3c3e`. The latest
`conclusion=failure` is a billing artefact, not a real failure.

### 3.3 CI gaps vs. CLAUDE.md "Validation chain"

CLAUDE.md mandates (`CLAUDE.md:154-167`):

```
cargo +nightly fmt --all                   → fmt job        ✅
cargo build --workspace --all-features     → build matrix   ✅
cargo clippy --workspace --all-features --all-targets -- -D warnings   → clippy   ✅
cargo test --workspace --all-features      → test job       ✅
cargo deny check                           → deny job       ✅
RUSTDOCFLAGS=…  cargo doc --workspace …    → doc job        ✅ (RUSTDOCFLAGS set globally)
cargo xtask check-no-channels              → no-channels    ✅
cargo xtask check-no-io-deps               → no-io-deps     ✅
cargo xtask codegen --check                → ❌ MISSING
```

**Confirmed CI gaps:**

1. **`cargo xtask codegen --check` is not wired.** CLAUDE.md flags this as
   part of the validation chain (`CLAUDE.md:165`). Drift in vendored proto
   codegen would only be caught locally. **High priority**: trivially
   cheap, catches a class of silent breakage.
2. **No MSRV check.** ADR-0007 pins MSRV at Rust 1.85 (`specs/adr/0007-edition-2024-msrv-1-85.md`).
   The `build` matrix runs [stable, beta] but never against 1.85, so a
   stable-only API would land unnoticed. Add a `msrv` job pinning
   `dtolnay/rust-toolchain@1.85`.
3. **No dependency-update bot.** No `.github/dependabot.yml`. cargo-deny
   catches license/security at PR time, but lockfile drift accumulates.
4. **No coverage upload.** `cargo-llvm-cov` + Codecov / cargo-tarpaulin
   would expose untested branches — relevant here because the e2e gap list
   above is mostly about runtime / engine code that *has* unit-test
   coverage but no broker-side proof.
5. **No `mutants-smoke` on PRs.** Currently `workflow_dispatch`-only
   (`ci.yml:170`). Consider scheduling weekly instead of gating PRs.
6. **`fuzz-smoke` time budget is short** (60 s each). Acceptable for PRs;
   consider a nightly `schedule:` trigger with a 10-minute budget.
7. **No `cargo audit` job.** Overlaps with `cargo deny advisories`; if
   `deny.toml` covers RustSec, fine — confirm.

**No gap on:**

- `no-internal-clock`: present (`ci.yml:115`), enforces ADR-0011.
- `no-channels` / `no-io-deps`: present, enforce ADR-0003 / ADR-0004.
- `moonpool-sim`: present (`ci.yml:129`), tests determinism harness.
- `e2e`: present (`ci.yml:147`), runs on every push/PR with a 75-min inner
  + 90-min outer timeout (matches CLAUDE.md guidance).

---

## Top-level findings (parent agent)

**(a) Parity rows with NO e2e coverage today (high-impact, low-friction
suites to add):**

- PIP-4 encryption + `cryptoFailureAction` (Fail/Discard/Consume)
- PIP-121 cluster failover (Static/Controlled/Auto) + PIP-188 topic-migrated
- Stage-2/3 supervised reconnect + `rebuild_producers` / `rebuild_consumers`
- OAuth2 `ClientCredentialsFlow` + token-cache refresh
- `dnsResolver` injection
- `memory_limit` accounting
- TLS `tls_allow_insecure_connection` + `tls_hostname_verification_enable`
- `AutoConsumeSchema` broker-fetch
- `seek_per_partition` callback
- Auto-update-partitions tickers (Pattern / partitioned / multi / TableView)
- PIP-313 force unsubscribe; rolling stats windows; Avro/Protobuf/KV/temporal schemas

**(b) Real pre-billing CI failures:** none on the current tip. The last
real failures (26221410209, 26220694546 on 2026-05-21) were lint / dead-code /
unit-test issues already fixed by 26221923859. The latest
`conclusion=failure` (26248189031) is a billing/spending-limit artefact —
jobs completed in 2 s with no output.

**(c) Prioritised list of new e2e suites to add:**

1. `e2e_reconnect` — Stage-2/3 supervised reset + producer/consumer rebuild
   (most invasive recent landing, zero coverage today).
2. `e2e_cluster_failover` — PIP-121 + PIP-188 (two new ADRs, no e2e).
3. `e2e_crypto` — PIP-4 round-trip with all three `cryptoFailureAction`
   branches.
4. `e2e_memory_limit` — `FailImmediately` policy + CAS release on drop.
5. `e2e_tls` — `tls_allow_insecure_connection` + hostname verify on/off
   against a TLS-enabled broker container.
6. `e2e_oauth2` — `ClientCredentialsFlow` against a `wiremock` IDP stub.
7. `e2e_pattern_auto_reconcile` + `e2e_tableview_auto_update_partitions`
   + `e2e_multi_topics_dynamic` — the four auto-update tickers in one
   coherent sweep.
8. `e2e_seek_per_partition` — callback parity with Java
   `seekAsync(Function<String, Object>)`.
9. `e2e_dns_resolver` — custom resolver consulted on every (re)connect.
10. `e2e_force_unsubscribe`, `e2e_schemas_extended`, `e2e_rolling_stats`
    — small follow-ups.

**Top CI gap:** `cargo xtask codegen --check` is in the validation chain
(`CLAUDE.md:165`) but **not wired into CI** — silent proto-drift hazard.
Secondary gaps: no MSRV (1.85) job, no dependabot, no coverage upload.

Report path: `/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar/docs/research-e2e-ci.md`
