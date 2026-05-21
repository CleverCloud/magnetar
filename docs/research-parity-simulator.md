# Research — Java Parity Gaps + Moonpool Simulator Coverage Gaps

Researcher: B (retry)
Date: 2026-05-21
Repository: /home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar
Scope: Inventory parity gaps against the Java client and moonpool engine
coverage gaps against the production tokio engine. Citations are
`file:line` against the repo tree at HEAD = `37d3c3e`.

---

## Part 1 — Java parity gaps

### 1.1 Source of truth

The parity matrix lives in `README.md` (`README.md:392-619`) and is
mirrored / refined by `docs/parity-status.md` (`docs/parity-status.md:1-150`).
Per ADR-0010 (`specs/adr/0010-v0-1-full-java-parity.md`), v0.1.0 ships
**full Java parity** — i.e. nothing 🟡 or ❌ in the matrix is, on its own,
v0.1.0-blocking *unless* `docs/parity-status.md` calls it out as such.
The `docs/parity-status.md:98-114` "Genuine deferred-scope items"
section is the authoritative deferral list.

### 1.2 Rows flagged 🟡 or ❌ in `README.md`

Pulled via `grep -nE '🟡|❌' README.md`:

| File:Line | Feature | Flag | v0.1.0-blocking? | Source |
| --- | --- | --- | --- | --- |
| `README.md:566` | SASL (Kerberos) | 🟡 | **No — deferred** | `docs/parity-status.md:104` |
| `README.md:567` | Athenz | 🟡 | **No — deferred** | `docs/parity-status.md:105` |
| `README.md:593` | `AutoProduceBytesSchema` | 🟡 | **No — deferred** | `docs/parity-status.md:106` |
| `README.md:647` | PIP-460 Scalable topics | ❌ | **No — M9** | `docs/parity-status.md:107` |
| `README.md:648` | PIP-466 V5 client API | ❌ | **No — M9 (eval only, not adopted verbatim)** | `docs/parity-status.md:108` |
| `README.md:649` | PIP-180 Shadow topic | ❌ | **No — M9** | `docs/parity-status.md:109` |
| `README.md:650` | PIP-415 `getMessageIdByIndex` | ❌ | **No — M9 (vendored proto bump)** | `docs/parity-status.md` (CLAUDE.md confirms) |
| `README.md:651` | PIP-33 Replicated subscriptions | ❌ | **No — M9** | `docs/parity-status.md:111` |

**Net result**: zero v0.1.0-blocking rows. All 🟡/❌ are explicitly
M9-deferred or post-v0.1.0. This matches the assertion in
`CLAUDE.md` ("What's landed vs. open" / "Open (deferred-scope only)").

### 1.3 Confirmation of the deferred set

Each item in the user-supplied deferred list maps cleanly to a row above:

- SASL Kerberos — `README.md:566` 🟡 (scaffold `magnetar-auth-sasl`)
- Athenz — `README.md:567` 🟡 (scaffold `magnetar-auth-athenz`)
- AutoProduceBytesSchema — `README.md:593` 🟡 (trait surface only)
- PIP-460 — `README.md:647` ❌
- PIP-466 — `README.md:648` ❌ (note: "inspired by, not adopted verbatim")
- PIP-180 — `README.md:649` ❌
- PIP-415 — `README.md:650` ❌
- PIP-33 — `README.md:651` ❌

All eight are confirmed deferred. None block v0.1.0.

### 1.4 ✅ rows that hide TODOs / caveats in Notes

A scan of the matrix for caveat phrasing ("still pending",
"planned follow-up", "user drives", "**Insecure**") surfaces:

1. **Producer `getStats`** — `README.md:418`. Caveat:
   `"Rolling per-second windows still pending."`
   *However*, three rows later (`README.md:447`) "Stats: rolling
   windows (msgs/sec, bytes/sec)" is itself ✅ with a long note
   describing the implementation. Inconsistency: the producer-stats
   row should be updated to drop the "still pending" sentence; the
   rolling-window row supersedes it. **Doc-drift bug, not parity gap.**
2. **Partitioned producer / consumer / multi-topics / table-view
   auto-update tickers** — `README.md:466, 474, 484, 514`. All ✅, but
   the Notes say "user drives `refresh_partitions(&client)` from the
   signal". This is a *deliberate* API choice (the ticker is a signal,
   not an automatic mutation), not a TODO. Worth checking that
   `magnetar` examples / docs document the signal-driven pattern.
3. **`memoryLimit`** — `README.md:611`. Caveat:
   `"ProducerBlock is the planned follow-up."` Java's
   `MemoryLimitPolicy` supports `FailImmediately` (shipped) and
   `ProducerBlock` (not yet). **Real, but partial-policy. Track as a
   v0.1.0 polish item, not a parity blocker** (ADR-0017 documents the
   atomic-CAS reservation; the policy enum exists).
4. **`tlsAllowInsecureConnection`** — `README.md:606`. Caveat:
   `"**Insecure**, do not use in production."` Behavioural warning,
   not a gap.

No ✅ row hides a load-bearing parity gap. Items (1) and (3) are
worth tracking; (2) and (4) are intentional.

---

## Part 2 — Moonpool simulator gap analysis

### 2.1 Moonpool engine public surface

`crates/magnetar-runtime-moonpool/src/` — six files (lib, client,
consumer, driver, producer, transport, tls). Public re-exports
(`crates/magnetar-runtime-moonpool/src/lib.rs:78-81`):

- `Client, ClientError, LookupTopicResult` (`client.rs:81`)
- `Consumer` (`consumer.rs:61`)
- `DriverHandle` (`driver.rs:109`)
- `Producer, SendFut` (`producer.rs:62, 338`)
- `ConnectionShared`, `TopicListChange`, `EngineError`,
  `MoonpoolEngine<P>` (`lib.rs:87, 141, 150, 170`)
- TLS adapter: `RustlsByteAdapter` (`tls.rs:41`)

That is the entire moonpool engine. No partitioned, multi-topics,
pattern, reader, table-view, transaction, OAuth2, DNS, failover, or
TopicMigrated surface exists in this crate.

### 2.2 Tokio engine public surface

`crates/magnetar-runtime-tokio/src/` — 12 files. Public re-exports
(`crates/magnetar-runtime-tokio/src/lib.rs:87-99`):

- `AutoClusterFailover, HealthProbe, HealthProbeFuture`
  (`auto_cluster_failover.rs:91, 47, 40`) — PIP-121 runtime side.
- `Client` (`client.rs:37`)
- `CompressionError` (`compress.rs`)
- `Consumer, ReceiveFut` (`consumer.rs:24, 969`)
- `EncryptError, MessageDecryptor, MessageEncryptor` (`crypto.rs`)
- `DnsResolveFuture, DnsResolver, TokioDnsResolver, arc_dns_resolver`
  (`dns.rs:35, 47, 57, 74`) — ADR-0015.
- `DriverHandle` (`driver.rs:142`) plus `spawn_supervised`
  (`driver.rs:226`) and `ReconnectContext` (`driver.rs:176`).
- `ClientError` (`error.rs`)
- `Producer, SendFut` (`producer.rs:26, 362`)
- `insecure_tls_config`, `tls_config_no_hostname`, `default_tls_config`
  (`tls_insecure.rs`, `tls_no_hostname.rs`, `transport.rs`)
- `ParsedUrl, Scheme` (`url_parse.rs`)
- `ConnectionShared` (`lib.rs:106`), `TopicListChange` (`lib.rs:149`)

Higher-level wrappers (PartitionedProducer, MultiTopicsConsumer,
PatternConsumer, Reader, TableView, Transaction, TypedConsumer/Producer)
live in `crates/magnetar/src/` (the façade), built **on top of** the
tokio engine's `Client/Producer/Consumer`. They are not in the moonpool
crate today.

### 2.3 Tokio-vs-moonpool diff (feature gap)

| Capability | Tokio | Moonpool | Gap |
| --- | --- | --- | --- |
| Single producer (`Producer::send`) | ✅ `producer.rs:26` | ✅ `producer.rs:62` | none |
| Single consumer (`Consumer::receive`) | ✅ `consumer.rs:24` | ✅ `consumer.rs:61` | none |
| Lookup (`Client::lookup_topic`) | ✅ | ✅ `client.rs:74` | none |
| TLS plumbing | ✅ `transport.rs`, `tls_insecure.rs`, `tls_no_hostname.rs` | adapter only (`tls.rs:41`); driver loop "drives plaintext path only" (`lib.rs:33-37`) | **moonpool TLS not wired into driver** |
| Supervised reconnect (`spawn_supervised`) | ✅ `driver.rs:226` + `ReconnectContext` `driver.rs:176` | ✗ `spawn` only (`driver.rs:158`) | **missing** |
| DNS resolver injection | ✅ `dns.rs:35-74` | ✗ — uses moonpool `NetworkProvider` directly | **missing trait wiring** |
| `AutoClusterFailover` runtime task | ✅ `auto_cluster_failover.rs:91` | ✗ | **missing** |
| `ServiceUrlProvider` consumed on reconnect | ✅ via supervised reconnect | ✗ (no supervised reconnect) | **missing** |
| PIP-188 `TOPIC_MIGRATED` reconnect | ✅ (driver-level) | ✗ | **missing** |
| `memory_limit` atomic CAS accounting | ✅ in `producer.rs` `SendFut` | ✗ in moonpool `producer.rs` / `SendFut` (`producer.rs:62, 338`) | **missing** |
| OAuth2 `ClientCredentialsFlow` wiring | ✅ via `ConnectionShared::auth_provider` | ✅ surface present (`lib.rs:99`) but no integration test | **untested** |
| Partitioned producer | (façade `magnetar/src/partitioned_producer.rs`) | ✗ | **missing** |
| Partitioned consumer | (façade) | ✗ | **missing** |
| Multi-topics consumer | (façade `multi_topics.rs`) | ✗ | **missing** |
| Pattern consumer (PIP-145) | (façade `pattern_consumer.rs`) | ✗ topic_list delta queue exists (`lib.rs:141, 156-159`) but no `PatternConsumer` wrapper | **missing wrapper** |
| Reader | (façade) | ✗ | **missing** |
| Table view | (façade `table_view.rs`) | ✗ | **missing** |
| Transactions (PIP-31) | proto-layer + façade `transaction.rs` | ✗ | **missing engine wiring** |
| Typed schema producer/consumer | (façade `typed.rs`) | ✗ | **missing** |

### 2.4 Existing moonpool test surface

`grep` of `#[test]` / `#[tokio::test]` under
`crates/magnetar-runtime-moonpool/`:

- `tests/error_mapping.rs` (`crates/magnetar-runtime-moonpool/tests/error_mapping.rs`)
- `tests/url_parse.rs`
- Inline `#[test]` units in `client.rs:341, 351, 361, 373, 391, 400, 410` (lookup result conversions, URL parsing).
- Inline `#[test]` units in `lib.rs:293, 301, 309, 329, 338` (`ConnectionShared` smoke).
- Inline `#[test]` in `consumer.rs:566` (single — likely state setup only).
- Inline `#[test]` in `producer.rs:657, 693` (two — likely `SendFut` polling state).
- Inline `#[test]` in `tls.rs:188, 204` (RustlsByteAdapter unit tests).

Counted: **~16 unit tests + 2 file-level integration tests**, almost
all of which exercise pure conversion / state-machine glue rather than
end-to-end producer↔consumer flows under chaos.

For comparison, `grep -rn '#\[tokio::test\]|#\[test\]|fn test_'`
under `crates/magnetar-runtime-tokio/` returns **37 tests** covering
auth refresh, DNS injection, supervised reconnect epochs, TLS variants,
URL parsing, dead-letter, redelivery, etc.

### 2.5 Coverage holes (chaos surface that the simulator should
unlock but currently doesn't exercise)

1. No moonpool test drives a full handshake → producer.send → ack →
   close cycle. (Inferred from per-file test counts above — no test
   imports both `Producer` and `Consumer`.)
2. No moonpool test exercises TLS at all in the driver (the adapter
   has unit tests but the driver loop says plaintext-only —
   `lib.rs:33-37`).
3. No test forces a network partition between handshake and first
   send to verify deterministic recovery, because the supervised
   reconnect path is absent (`driver.rs:158-193` has only `spawn`,
   not `spawn_supervised`).
4. No test reorders frames within a connection to verify the
   sans-io state machine wakes the right `Waker` — exactly the chaos
   the simulator was designed for.
5. No PIP-145 topic-list-changed coverage despite the queue +
   notify being plumbed (`lib.rs:141, 156-159`).
6. No transaction coordinator interaction test.
7. No clock-driven test for `ack_group_time` / send-timeout /
   chunked-message deadlines using moonpool's virtual `TimeProvider`
   — the prime reason the simulator exists per `lib.rs:8-16`.

### 2.6 Proposed moonpool-M5 → moonpool-M8 milestones

**moonpool-M5 — Wire the missing engine primitives (parity with tokio
engine surface):**

- Port `spawn_supervised` + `ReconnectContext` from
  `magnetar-runtime-tokio/src/driver.rs:176-377` to the moonpool
  driver (`crates/magnetar-runtime-moonpool/src/driver.rs:158-193`).
- Add `DnsResolver` trait (mirror `dns.rs:47`) backed by
  `NetworkProvider::resolve` so the simulator can inject failure
  patterns.
- Drive TLS through the driver loop (plumb `RustlsByteAdapter` from
  `tls.rs:41` into `transport.rs:26`).
- Wire `memory_limit` atomic-CAS reservation into moonpool
  `Producer::send` / `SendFut::Drop` (parity with ADR-0017 in tokio's
  `producer.rs`).
- Plug `ServiceUrlProvider` into the supervised-reconnect path, and
  handle `TOPIC_MIGRATED` (ADR-0018).

**moonpool-M6 — Lift the façade wrappers onto moonpool:**

The façade crates in `crates/magnetar/src/` (`partitioned_producer.rs`,
`partitioned_consumer.rs`, `multi_topics.rs`, `pattern_consumer.rs`,
`table_view.rs`, `transaction.rs`, `typed.rs`) are written against the
tokio engine's `Client`/`Producer`/`Consumer`. Either:

- (a) Make them generic over an `Engine` trait (one-time refactor), or
- (b) Provide a parallel moonpool-flavoured façade.

Decision per ADR-0010 = full parity, so (a) is preferable. This is the
single biggest piece of moonpool work.

**moonpool-M7 — Chaos test pack:**

Build deterministic tests covering:

- Mid-handshake partition + recovery (epoch bump check against
  `Connection::reset`).
- Frame reordering between producer-create and send-ack.
- Clock-driven `ack_group_time`, `send_timeout`, chunked redelivery.
- `TOPIC_MIGRATED` mid-publish.
- OAuth2 token expiry mid-connection (using virtual clock to fast-forward
  past expiry-30 s refresh window).
- PIP-145 topic-list churn under flapping broker.
- PIP-121 `AutoClusterFailover` health-probe oscillation.

**moonpool-M8 — Equivalence harness:**

Property-test harness that drives the same script through both engines
and asserts byte-for-byte equality of outbound frames + ordered event
streams. This is the long-term differential testing surface the
simulator was built to enable; only feasible once M5/M6 land.

---

## Open questions / risks

- Should the M6 façade refactor (engine generic) be lumped into
  v0.1.0 or done post-launch? ADR-0010 says full parity, but the
  parity matrix in `README.md` is silent on which engine satisfies
  each row. Worth a short ADR clarifying that the matrix is satisfied
  by the **tokio** engine for v0.1.0, with moonpool parity as a
  follow-up milestone train.
- `magnetar-auth-sasl` and `magnetar-auth-athenz` are crate scaffolds
  (`README.md:566-567`). Confirm with `docs/parity-status.md:104-105`
  that pre-alpha status is the agreed v0.1.0 ship state.
- `AutoProduceBytesSchema` 🟡 — is the trait surface enough for
  v0.1.0 (`README.md:593`), or do we need at least one round-trip
  test before tagging?
- The `getStats` row at `README.md:418` should be updated to remove
  the "Rolling per-second windows still pending" sentence, since
  `README.md:447` ships that feature. Doc cleanup, not code work.

---

## Source citations summary

- Parity matrix: `README.md:392-619`.
- 🟡/❌ rows: `README.md:566,567,593,647,648,649,650,651`.
- Deferral authority: `docs/parity-status.md:98-114`.
- Caveat rows worth tracking: `README.md:418, 611, 606`.
- Moonpool engine surface: `crates/magnetar-runtime-moonpool/src/lib.rs:66-170`,
  `client.rs:47-314`, `consumer.rs:61-424`, `driver.rs:109-193`,
  `producer.rs:62-429`, `tls.rs:41-204`, `transport.rs:26`.
- Tokio engine surface: `crates/magnetar-runtime-tokio/src/lib.rs:67-149`,
  `auto_cluster_failover.rs:40-91`, `client.rs:37`, `consumer.rs:24,969`,
  `dns.rs:35-74`, `driver.rs:142-377`, `producer.rs:26,362`.
- Façade wrappers (tokio-only today):
  `crates/magnetar/src/{partitioned_producer.rs,partitioned_consumer.rs,multi_topics.rs,pattern_consumer.rs,reader.rs (in client.rs),table_view.rs,transaction.rs,typed.rs}`.
- Moonpool test surface: `crates/magnetar-runtime-moonpool/tests/{error_mapping.rs,url_parse.rs}`
  and inline `#[test]` blocks listed in §2.4.
