# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Admin client (`magnetar-admin`):** non-JSON admin responses now surface the request method, URL, HTTP status, `Content-Type`, and a truncated body snippet instead of the bare `serde_json` message (`json decode: expected value at line 1 column 1`). Hitting the wrong endpoint, a reverse proxy, or an auth-redirect on a 2xx is now self-diagnosing. Non-success statuses (`AdminError::Status`) also name the method + URL. (#282)

### Changed

- **`magnetar-admin` (`AdminError`):** added a new `AdminError::Decode { method, url, status, content_type, snippet, source }` variant carried by the JSON decoders, and added `method: String` + `url: String` fields to the existing `AdminError::Status` variant. `AdminError` stays exhaustive (no `#[non_exhaustive]`), so any exhaustive `match` over it or any `Status { code, body }` destructure without `..` must be updated. The existing `AdminError::Json` variant is now reserved for request-body **encode** failures only (its `#[error]` text changed from `json decode: …` to `json encode: …`); response **decode** failures route through `AdminError::Decode`. (#282)

## [1.0.1] - 2026-06-15

### Changed

- Renamed the published crates.io packages to avoid a name collision (the `magnetar` name is held by an unrelated, abandoned crate): the façade ships as **`magnetar-driver`** and the CLI as **`magnetarctl`** (binary command `magnetarctl`). The façade's library/import name is unchanged — `use magnetar::*` still works; only the dependency line differs (`magnetar-driver = "1.0.1"`). No API, behavior, or wire-format change. (ADR-0067)

## [1.0.0] - 2026-06-15

First stable release.
Magnetar is a from-scratch Apache Pulsar client driver for Rust with full Apache Pulsar Java-client parity, a sans-io protocol core, and two interchangeable runtime engines.
See the [parity matrix](README.md#java-client-parity-matrix) for the per-feature status snapshot.

### Added

- Initial public release of magnetar, a from-scratch Apache Pulsar client driver for Rust. Targets Apache Pulsar 4.0+ LTS, advertises CONNECT `ProtocolVersion::V21` with downgrade fallback, and ships as a 12-crate workspace (façade `magnetar`, sans-io core `magnetar-proto`, `magnetar-runtime-tokio`, `magnetar-runtime-moonpool`, `magnetar-differential`, `magnetar-admin`, `magnetarctl`, `magnetar-fakes`, `magnetar-auth-oauth2`, `magnetar-auth-sasl`, `magnetar-auth-athenz`, `magnetar-messagecrypto`) plus `xtask`. (75f7c16)
- Sans-io protocol core (`magnetar-proto`): a `quinn-proto`-style pure state machine (`handle_bytes` / `poll_transmit` / `poll_event` / `poll_timeout`) for Connection, Producer, and Consumer, with zero I/O dependencies and injected clocks (`now: Instant`, `wall_clock` provider). The same state machine drives both engines. (123b8db, 10cb025; ADR-0004, ADR-0011)
- Dual runtime engines selected at the type level via `PulsarClient<E: Engine = TokioEngine>`: a production tokio engine and a deterministic-simulation moonpool engine over `moonpool_core::Providers`. The moonpool engine reaches full façade parity (driver loop + transport, Client lookup / partitioned-metadata / topic-watch, Producer send/flush/close, Consumer receive/ack/seek) plus a rustls-over-bytepipe TLS adapter. (405d2cd, 9555113, 1eba8e1, f59032f, e01d676, 3a119e7)
- No-channels concurrency architecture: all `mpsc`/`broadcast`/`watch`/`oneshot` and third-party channel crates are banned and replaced with `Arc<parking_lot::Mutex<…>>` + `tokio::sync::Notify` + `Waker` slabs. A split connection mutex enforces global→per-slot lock ordering so the `Producer::send` hot path takes only the per-slot mutex. (3275b41; ADR-0003, ADR-0038)
- Producer Java-client parity: `send`/`sendAsync`, batching with `batchingMaxPublishDelay` flush timer, message chunking (PIP-37, chunks-never-batched with bounded consumer reassembly cap), LZ4/ZSTD/Snappy/ZLIB compression, `initialSequenceId`, `sendTimeout`, producer access modes Shared/Exclusive/WaitForExclusive/Fencing (PIP-68), custom `MessageRouter` with Murmur3/JavaStringHash, an interceptor SPI, `TypedMessageBuilder`, and hdrhistogram p50/p99/max stats. Best-effort `CloseProducer` is sent on last-clone drop. (#243; ADR-0057, ADR-0063)
- Consumer Java-client parity: Exclusive/Shared/Failover/Key_Shared subscriptions, `receive`/`batchReceive`, the full ack family (individual/cumulative/batch/with-properties/under-txn) including batch-index ack (PIP-54/391), negative-ack with `MultiplierRedeliveryBackoff` and an ack-timeout tracker (PIP-37), `reconsumeLater` retry-letter (PIP-58), dead-letter policy (PIP-22/58/124/409), seek by id/timestamp/earliest/latest and per-partition, pause/resume, `readCompacted`, key-shared sticky/auto-split/hash policy (PIP-34/119/282/379), subscription properties, `replicateSubscriptionState`, force-unsubscribe (PIP-313), and `MessageListener` push delivery across single/typed/multi-topic/partitioned/pattern consumers. (fe33784; ADR-0064)
- Reader, partitioned producer/consumer, multi-topic, pattern (regex, PIP-145), and `TableView` surfaces, all generic over `E: Engine`, with `auto_update_partitions_interval` tickers for partition growth. (8cfd1e3, 844655b, fe5d8c0, b51680a, 31f9cbe, 2b7570c, f09f23c)
- Transactions (PIP-31) end-to-end: a `TxnClient` coordinator with begin/commit/abort, `ADD_PARTITION_TO_TXN` / `ADD_SUBSCRIPTION_TO_TXN`, publish-under-txn, ack-under-txn, and `END_TXN` cleanup; the `Transaction` surface is engine-generic. (71e81e9, 19a8df5, ab9041b)
- Schema layer with full Java parity: Bytes/String/JSON/Avro/Protobuf/ProtobufNative/KeyValue/AutoConsume (PIP-87 broker lookup)/AutoProduceBytes plus all primitives. AVRO/JSON are canonicalised via the broker canonical form (apache-avro 0.21); PROTOBUF_NATIVE and KeyValue output is byte-identical to the Java client. (f3eb61b, d265a06, 08f5702)
- Authentication provider parity: Token, mTLS, OAuth2 `ClientCredentialsFlow` with token caching, SASL-PLAIN (RFC 4616), SASL-Kerberos via `libgssapi` multi-round `AUTH_CHALLENGE`, and Athenz (pre-fetched role token plus opt-in ZTS round-trip). In-band `AUTH_CHALLENGE` credential refresh implements PIP-30/PIP-292. (48a65b4, 122298e; ADR-0014, ADR-0029, ADR-0030, ADR-0041)
- Pluggable rustls crypto providers selected at compile time on the façade: `crypto-aws-lc-rs` (default, post-quantum hybrid key exchange), `crypto-ring`, `crypto-openssl` (`rustls-openssl` wrapper), and `crypto-fips` (`aws-lc-fips-sys`). rustls-only — no native-tls. (3f392af, b6f9cbe, closes issue #9; ADR-0005, ADR-0035)
- End-to-end message encryption (PIP-4, `magnetar-messagecrypto`): AES-GCM payload encryption with RSA-OAEP key wrapping, `MessageEncryptor`/`MessageDecryptor` traits on producer and consumer, and `cryptoFailureAction` Fail/Discard/Consume wired end-to-end, including a moonpool message-crypto bridge. (1bfc7e3, 6039251; ADR-0044)
- Admin REST client (`magnetar-admin`, reqwest + rustls) and a kubectl-style `magnetar` CLI: namespace/topic policy endpoints (retention, backlog-quota, TTL, persistence, dispatch-rate, dedup, compaction, delayed-delivery, max-producers/consumers/unacked), schema registry, rack-aware brokers/bookies, Functions/Sources/Sinks/Packages, subscription ops, and PIP-415 `getMessageIdByIndex`. (d315c20, d26028b)
- Resilience: supervised reconnect with `Connection::reset` and transparent producer/consumer rebuild, keepalive watchdog, terminal fast-fail, lookup-retry on session-lost, ack-gated re-attach replay, and a handshake-failure budget. Memory limit with `FailImmediately` atomic CAS and a `ProducerBlock` `Waker` slab, cluster failover (PIP-121 `ServiceUrlProvider`, Controlled/Auto), and `TOPIC_MIGRATED` supervised reconnect (PIP-188). (#263, 5dcc6f9, 6013320; ADR-0016, ADR-0017, ADR-0018, ADR-0020, ADR-0028, ADR-0060, ADR-0061)
- Additional PIPs: broker-entry metadata (PIP-90), shadow topics (PIP-180 — admin CRUD, producer `send_with_source_message_id`, consumer `MessageReceivedFromShadow`), and replicated subscriptions (PIP-33 — `replicate_subscription_state` field plus marker filter, with a two-cluster e2e fixture). (bc7ea94, 01d0afd; ADR-0033, ADR-0034)
- Experimental, default-off surfaces: PIP-466 V5 client (`magnetar::v5` behind `experimental-v5-client`, wraps v4 with no wire change) and the PIP-460 scalable-topics scaffold (behind `scalable-topics`: `topic://` URLs, DAG watch, `StreamConsumer`, `magnetar topic-info`). No released broker ships PIP-460. (b3c581e, d3684ac; ADR-0031, ADR-0032)
- Observability and proxy support: OpenTelemetry context propagation behind the `opentelemetry` feature (auto-injects `traceparent`/`tracestate` into message properties at the send boundary), and Apache Pulsar Proxy support via a per-broker connection pool with lookup-driven routing. (#151, #17; ADR-0039, ADR-0053)
- Structured logging across the driver: every error/warn/info log carries at least one structured field (xtask-enforced), with subscriber-side rate-limiting/sampling guidance. (#218, #280; ADR-0054, ADR-0065)
- Cross-runtime test and coverage policy: every behavioral change ships proto-unit + tokio-integration + moonpool-integration + differential-equivalence + e2e tests, with 100% moonpool patch coverage and a strict 1:1 tokio↔moonpool test count, all xtask-gated. The deterministic-simulation harness adds buggify fault injection, swizzle-clog workload, bit-flip survivability, and a seed sweep. (0c8c26c, fec933b; ADR-0024, ADR-0036, ADR-0048, ADR-0050, ADR-0055)

### Changed

- Migrated to Rust Edition 2024 and raised the MSRV to 1.88. (ADR-0007, ADR-0042)
- Refactored the client façade into dedicated `builders.rs` / `client_builder.rs` modules. (9b83a00, a69830c)
- Repinned the moonpool dependency to the published crates.io 0.7.0 (from a floating git dependency) and adopted vectored writes. (#242, 6a0e24b; ADR-0043, ADR-0056)
- Bumped runtime dependencies: `libgssapi` 0.7.2→0.9.1, `http` 1.4.0→1.4.1, and `rcgen` 0.13.2→0.14.8. (#13, #152)

### Removed

- Removed dead scaffolding and consolidated the crypto traits into `magnetar-proto`. (2a07f07)
- Removed `tls_trust_certs_file_path` from `ClientBuilder`. (1736be8)
- Dropped the earlier multi-step (0.1.0 / 0.2.0) release-planning artifacts, superseded by this single 1.0.0 release. (fd6e62d)

### Fixed

- Pre-release audit correctness fixes: decompression-bomb size cap, `Instant`-overflow guard, partition-hash correctness, and multi-topic receive starvation. (#279, 9347c39)
- Closed transaction parity gaps so the transactions e2e suite passes. (19a8df5)
- Hardened consumer behavior during seek and across transient broker-close, and fixed partitioned-topic auto-detection on topic delete. (issue #65, 780349c, 6ec47a1)
- The ack-timeout tracker now drops nacked message ids to prevent double redelivery. (7ce5e25)
- Resolved moonpool deterministic-simulation seed-sweep failures and hardened swizzle seed replay. (#244, #262, #264)
- The reconnect supervisor now persists its backoff across reconnects and resets only after the drop-grace window is stable. (#16)

### Security

- Secrets are redacted from `Debug` output: passwords and private keys (CWE-532), Athenz `private_key_pem`, and `AdminAuth::Token`, each guarded by secret-scan log-capture tests. (3406f7d, e92994e, f5ae060, 28711ef)
- All `panic!` and `debug_assert!` calls are removed from `magnetar-proto` production paths; every path returns `Result`/`Option`. (a561203, cac2199)
- CRC32C verify-or-drop on frames with magic `0x0e01`: a checksum mismatch emits a `ChecksumMismatch` event and drops the frame.
- Exposed `tls_allow_insecure_connection` and `tls_hostname_verification_enable` for Java parity, and cleared cargo-audit advisories (`time` 0.3.45 CVE, `rustls-pemfile` unmaintained). (2a9fafb, abc7aad)

[1.0.1]: https://github.com/CleverCloud/magnetar/releases/tag/v1.0.1
[1.0.0]: https://github.com/CleverCloud/magnetar/releases/tag/v1.0.0
