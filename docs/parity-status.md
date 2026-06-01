# Java Parity Status

The authoritative parity matrix lives in
[`../README.md#java-client-parity-matrix`](../README.md#java-client-parity-matrix).
This document gives the matrix its engine-by-engine cut and notes the
items whose surface is gated behind an opt-in feature or stays
explicitly out of scope.

Per [ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md),
the Java parity matrix is satisfied **by the tokio engine**. The
moonpool engine reaches feature parity with tokio on its own
follow-up train; the gap is tracked below.

## Engine surface

| Surface | tokio | moonpool |
| --- | --- | --- |
| Engine driver loop + transport scaffold | ✅ | ✅ |
| Vectored producer-batch writes ([ADR-0040](../specs/adr/0040-vectored-io-transmit-enum.md)) | ✅ (`writev` on plaintext) | ✅ (real `futures` `write_vectored`: segment-granular under `SimProviders`, single-write fallback under `TokioProviders`' `Compat`; TLS contiguous — [ADR-0043](../specs/adr/0043-temporary-floating-moonpool-git-dep.md)) |
| Client (lookup + partitioned-metadata + topic-watch) | ✅ | ✅ |
| Producer façade (send / flush / close) | ✅ | ✅ |
| Consumer façade (subscribe / receive / ack) | ✅ | ✅ |
| PIP-4 message encryption + decryption (AES-GCM) with `CryptoFailureAction` ([ADR-0044](../specs/adr/0044-moonpool-message-crypto-bridge.md)) | ✅ | ✅ (encrypt-on-send / decrypt-on-receive bridge; Fail / Discard / Consume arms; equivalence in `magnetar-differential`) |
| Supervised reconnect (Stage 2 + Stage 3 rebuild) | ✅ | ✅ (multi-cycle redial coverage via `supervised_redial.rs`) |
| DNS resolver injection ([ADR-0015](../specs/adr/0015-dns-resolver-injection.md)) | ✅ | ✅ |
| Driver-level TLS (rustls byte-pipe — [ADR-0006](../specs/adr/0006-moonpool-tls-byte-pipe.md)) | ✅ | ✅ |
| `memory_limit` atomic-CAS reservation ([ADR-0017](../specs/adr/0017-memory-limit-atomic-reservation.md)) | ✅ | ✅ |
| `MemoryLimitPolicy::ProducerBlock` ([ADR-0020](../specs/adr/0020-memory-limit-producer-block.md), [ADR-0022](../specs/adr/0022-memory-limit-producer-block-moonpool.md)) | ✅ | ✅ |
| `ServiceUrlProvider` + `ControlledClusterFailover` ([ADR-0016](../specs/adr/0016-pip-121-cluster-failover.md)) | ✅ | ✅ |
| `AutoClusterFailover` (PIP-121 with `HealthProbe`) | ✅ | ✅ |
| PIP-188 `TOPIC_MIGRATED` → reconnect ([ADR-0018](../specs/adr/0018-pip-188-reconnect-on-migrate.md)) | ✅ | ✅ |
| Generic `PulsarClient<E: Engine>` ([ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md)) | ✅ | ✅ |
| Partitioned producer | ✅ | ✅ (engine-generic; tokio-only `refresh_partitions` + batch counters on specialisation) |
| Partitioned consumer | ✅ | ✅ (engine-generic via `MultiTopicsConsumer<C>` alias + `PartitionedConsumerBuilder<'a, E>`) |
| MultiTopicsConsumer | ✅ | ✅ (engine-generic `MultiTopicsConsumer<C>` + `MultiTopicsConsumerBuilder<'a, E>`) |
| PatternConsumer (PIP-145) | ✅ | ✅ (engine-generic `PatternConsumer<C>` + `PatternConsumerBuilder<'a, E>`; PIP-145 child-subscribe via `<E::ClientState as SubscribeApi>::subscribe`) |
| Reader | ✅ | ✅ |
| TableView | ✅ | ✅ |
| Transactions (PIP-31) | ✅ | ✅ |
| Typed schemas | ✅ | ✅ |
| Binary proxy (`proxy_to_broker_url`, [ADR-0039](../specs/adr/0039-pulsar-proxy-multi-broker-connection-model.md)) | ✅ (`ProxyConnectionPool` pins per-broker connections, avoids the ~90 ms reconnect storm from issue #15) | 🟡 (moonpool lookup path returns `ProxyUnsupportedOnUnsupervisedClient`; the moonpool flavour of `ProxyConnectionPool` is tracked in [`follow-ups.md §3`](follow-ups.md#3-moonpool-proxyconnectionpool-parity)) |
| Deterministic chaos pack | n/a | ✅ |
| tokio ↔ moonpool differential equivalence harness | n/a | ✅ |

`MoonpoolEngine<P>` is generic over the
[`moonpool_core::Providers`](https://crates.io/crates/moonpool-core)
bundle. `TokioProviders` runs it against a real broker;
`moonpool-sim`'s `SimProviders` runs it under deterministic seeds
([`moonpool-engine.md`](moonpool-engine.md)).

**All dependent façade surfaces are lifted** per ADR-0026 §D1
and now work on both engines:

- **Transaction (PIP-31)** via the `TransactionApi` extension
  trait.
- **Reader** via `Reader<C: ConsumerApi>` (default
  `C = magnetar_runtime_tokio::Consumer`).
- **TableView** via `TableView<C: ConsumerApi + Clone>`.
- **PartitionedProducer** via `impl<P: ProducerApi>
  PartitionedProducer<P>` (with tokio-only specialisation for
  `refresh_partitions`, batch counters,
  `last_sequence_id_published`).
- **MultiTopicsConsumer / PartitionedConsumer** via
  `MultiTopicsConsumer<C: ConsumerApi>` (default `C =
  magnetar_runtime_tokio::Consumer`) +
  `MultiTopicsConsumerBuilder<'a, E: Engine = TokioEngine>` /
  `PartitionedConsumerBuilder<'a, E>`; `.subscribe()` routes
  through the engine-generic `ConsumerBuilder` (which dispatches
  via `SubscribeApi`) and `partitions_for_topic` dispatches
  through the new `BrokerMetadataApi` extension trait.
- **PatternConsumer (PIP-145)** via `PatternConsumer<C: ConsumerApi>`
  + `PatternConsumerBuilder<'a, E: Engine = TokioEngine>`;
  PIP-145 auto-reconcile (`update()` + `start_auto_reconcile()`)
  subscribes child consumers through
  `<E::ClientState as SubscribeApi>::subscribe` and polls
  `TopicListChanged` deltas through
  `<E::ClientState as BrokerMetadataApi>::poll_topic_list_change`.

**TypedProducer / TypedConsumer** are also engine-generic at the
struct *and* builder level:
`TypedProducerBuilder<'a, S, E: Engine = TokioEngine>` /
`TypedConsumerBuilder<'a, S, E: Engine = TokioEngine>` build via
`E::ClientState: CreateProducerApi` / `SubscribeApi`. The
`partitioned_producer`, `table_view`, and `typed_table_view`
entry-points are now in the engine-generic `PulsarClient<E>` block.

The **base** `ConsumerBuilder<'a, E: Engine = TokioEngine>` /
`ProducerBuilder<'a, E: Engine = TokioEngine>` /
`ReaderBuilder<E: Engine = TokioEngine>` are engine-generic
(landed via `SubscribeApi` / `CreateProducerApi` extension traits,
commits `cc61d4d`, `0b6f363`, `08c89ca`).

The `ConsumerApi` trait surface is now comprehensive (pass-2
extension): receive, ack, ack_cumulative, negative_ack,
negative_ack_with_delay, ack_grouped, ack_grouped_cumulative,
ack_with_txn, ack_cumulative_with_txn, redeliver_unacked,
`unsubscribe(force: bool)`, seek_to_earliest, seek_to_latest,
`seek_to_message`, `seek_to_timestamp`, `pause`, `resume`,
`available_in_queue`, `available_permits`,
`has_received_any_message`, `has_reached_end_of_topic`,
`is_paused`, `is_inactive`, `drain_dead_letter`,
`receive_with_timeout`, `receive_batch`,
`receive_batch_with_bytes_cap`, `republish_dead_letters`,
`reconsume_later`, `reconsume_later_with_properties`, get_schema,
last_message_id, has_message_after, last_disconnected_timestamp,
topic, subscription, name, is_closed, is_connected, stats,
close_owned. The DLQ + retry helpers thread a matched
`type Producer: ProducerApi<Error = Self::Error>` associated
type so the trait stays runtime-typed without re-introducing a
tokio-only carve-out.

The companion `BrokerMetadataApi` extension trait (added alongside
pass-2) lifts the partition-count + topic-list watcher lookups —
`partitioned_topic_metadata`, `watch_topic_list`,
`poll_topic_list_change` — onto each engine's `Client` so
[`PartitionedConsumerBuilder`] / [`PatternConsumerBuilder`] /
`PulsarClient::partitions_for_topic` /
`PulsarClient::topic_list_snapshot` are all engine-generic.

Callers that reach for a tokio-only method on the moonpool engine
still get a trait-bound compile error, not a silent fallback —
see ADR-0019 §Consequences.

## Surface coverage

Everything with a `🟡` or `❌` in the README parity matrix is one of:

| Item | Status | Notes |
| --- | --- | --- |
| **SASL `PLAIN` (RFC 4616)** | ✅ supported | `magnetar_auth_sasl::SaslPlain` emits the `\0<user>\0<pass>` payload. Useful for username/password broker auth in tests and for brokers configured without a token provider. |
| **SASL Kerberos / GSSAPI** | ✅ supported | `magnetar_auth_sasl::SaslKerberos` binds `libgssapi` under the `auth-sasl-kerberos` façade feature. The multi-round `AUTH_CHALLENGE` continuation threads through `AuthProvider::respond_to_challenge`. The four sans-io test layers per [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md) drive a `ScriptedGssapiClient`; e2e uses a Dockerised KDC. See [ADR-0029](../specs/adr/0029-sasl-kerberos-gssapi-scope.md). |
| **Athenz (pre-fetched role token)** | ✅ supported | `AthenzProvider::with_role_token` uses the supplied token as the `auth_data` payload — useful when an out-of-band agent (`zts-agent`, sidecar) already mints the token. |
| **Athenz (ZTS round-trip)** | ✅ supported (`feature = "auth-athenz-zts"`, default off; built-in signer behind `crypto-aws-lc-rs` / `crypto-ring`) | The pluggable `zts::ZtsClient` trait is the HTTPS seam — `zts::HttpZtsClient` does the reqwest-backed `POST /zts/v1/oauth2/token` (or legacy `n-token`) exchange — while `AthenzProvider` owns the `parking_lot`-guarded expiry-aware cache + the sans-io `ensure_role_token(now)` / `needs_refresh(now)` state machine ([ADR-0011](../specs/adr/0011-clock-injection-sans-io.md) clock injection; `wall_clock` makes the JWT `iat`/`exp` deterministic). JWT signing has two paths: (a) `jwt_signer::AwsLcRsSigner` / `jwt_signer::RingSigner` are wired in-tree via `AthenzProvider::with_default_signer(config)`, gated on the crypto-provider matrix per [ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md); or (b) a caller-supplied `zts::JwtSigner` / `zts::ZtsClient` via `AthenzProvider::builder()`. Parsed PKCS#8 DER wrapped in `zeroize::Zeroizing<…>`; byte-identical deterministic RS256 (RFC 8017 §8.2), cross-backend equivalence pinned by `ring::tests::cross_backend_signature_byte_identity`. **Full four-layer cross-runtime coverage** ([ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md), [ADR-0041](../specs/adr/0041-athenz-provider-testability-seams.md)): tokio (`athenz_zts_round_trip.rs`, real wiremock), moonpool (`athenz_refresh_edge.rs`, scripted `ZtsClient` + injected `now`), differential (`athenz_auth_data_equivalence.rs`, byte-identical JWT + auth_data), plus the e2e fixture in [`crates/magnetar/tests/e2e_athenz_zts.rs`](../crates/magnetar/tests/e2e_athenz_zts.rs) (three wiremock-stub tests + a Docker reachability probe against `athenz/athenz-zts-server:1.12.41`). The production-style ZMS+ZTS+cert-bootstrap topology stays out of scope (would need the Athenz `make deploy-dev` 4-container stack as a shared CI fixture). |
| **PIP-460** — Scalable topics | 🟡 experimental (scaffold, feature-gated) | Behind `feature = "scalable-topics"` (default off). **No released Pulsar broker speaks PIP-460 today** — upstream PIP is `Draft`, targets Pulsar 5.0 LTS. Magnetar provides the **client-side scaffold**: hand-encoded wire commands (`CommandScalableTopicLookup` / `CommandSegmentDagWatch` / `CommandSegmentDagUpdate` + responses behind the feature gate until the upstream RC vendor bump), the `DagWatchSession` sans-io state machine (monotonic `update_seq`, split / merge / removal), the additive default-`None` `MessageId::segment_id` field (wire byte-identical when `None`), both-engine `ScalableTopicsApi` impls, the `magnetar::scalable::StreamConsumer` surface (StreamConsumer-only, **drops on DAG change** — no transparent failover), and the `magnetar topic-info` CLI subcommand. Four-layer test set per [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md): proto unit + tokio + moonpool 1:1 + differential equivalence (golden trace `scalable_topic_drop_on_split.json`). E2E (`crates/magnetar/tests/e2e_scalable_topic.rs`) is `#[ignore]`'d behind `feature = "e2e,scalable-topics"` — it can only run once upstream cuts a 5.0 RC. QueueConsumer / CheckpointConsumer / controller-election / in-place repartition stay out of scope until the broker side stabilises. See [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md) + [`docs/scalable-topics.md`](scalable-topics.md). |
| **PIP-466** — V5 client surface | ✅ experimental (engine-generic) | Behind `feature = "experimental-v5-client"` (default off). `magnetar::v5` exposes `PulsarClientV5<E: Engine = TokioEngine>` (v4 escape hatch via `v4()` / `into_v4()`), `v5::Producer<E>`, `v5::StreamConsumer<E>` (Exclusive / Failover), `v5::QueueConsumer<E>` (Shared / `KeyShared`), and the `v5::mapping` field-translation table (`Duration` / `Option<Duration>` / `Option<usize>` ↔ v4 millis-as-`u64` / `usize`). Moonpool callers name `PulsarClientV5<MoonpoolEngine<P>>` directly. No wire change, no sans-io change. Java V5 still iterating upstream — magnetar's surface targets Pulsar 4.x compatibility. See [ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md). |
| **PIP-180** — Shadow topic | ✅ supported | [ADR-0033](../specs/adr/0033-pip-180-shadow-topic-scope.md), [`docs/shadow-topic.md`](shadow-topic.md). Three admin REST methods (`create_shadow_topic` / `delete_shadow_topic` / `get_shadow_topics` + `get_shadow_source`), producer-side `send_with_source_message_id` propagating `CommandSend.message_id`, consumer-side `MessageReceivedFromShadow` event, structural `MessageId` equality across source ⇄ shadow. No proto bump. Caveat: source `MessageId` is client-asserted (broker validates write auth but does not prove the id is from a real source entry — upstream behaviour). The **replicator-side** path is exercised end-to-end by [`crates/magnetar/tests/e2e_shadow_topic_replicator.rs`](../crates/magnetar/tests/e2e_shadow_topic_replicator.rs) against a self-hosting single-cluster fixture (token auth + `replicator` role grant + in-container shadow create): it pins the broker's two orthogonal gates — authorisation (producer attach) and topic-type (`send_with_source_message_id` rejected with `code 22` on a regular topic, accepted on a shadow). See [`docs/shadow-topic.md` §Replicator-role e2e setup](shadow-topic.md#replicator-role-e2e-setup). |
| **PIP-33** — Replicated subscriptions | ✅ supported | `ConsumerBuilder::replicate_subscription_state(bool)` flips `CommandSubscribe` field 14; receive-path filter drops `REPLICATED_SUBSCRIPTION_*` markers and emits `ConnectionEvent::ReplicatedSubscriptionMarkerObserved`. Snapshot generation stays broker-side per [ADR-0034](../specs/adr/0034-pip-33-replicated-subscriptions-scope.md). Two-cluster e2e runs weekly via [`.github/workflows/e2e-replicated-subs.yml`](../.github/workflows/e2e-replicated-subs.yml). Docs: [`docs/replicated-subscriptions.md`](replicated-subscriptions.md). |

The Java-parity baseline ([ADR-0010](../specs/adr/0010-v0-1-full-java-parity.md))
is satisfied on the tokio engine.

## Constraints recap

- **No channels.** `tokio::sync::{mpsc,broadcast,watch,oneshot}`,
  `std::sync::mpsc`, `crossbeam-channel`, `flume`, `async-channel`,
  `kanal`, `postage`, `tachyonix`, `thingbuf` — banned. Use
  `Arc<parking_lot::Mutex<...>>` + `tokio::sync::Notify` +
  `core::task::Waker` slabs. See
  [ADR-0003](../specs/adr/0003-no-channels-rule.md).
- **Sans-io clock injection.** Every `magnetar-proto::Connection` entry
  takes `now: Instant` plus a `wall_clock` provider. See
  [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md).
- **`rustls` only.** No `native-tls`, no `openssl`. See
  [ADR-0005](../specs/adr/0005-rustls-only-tls.md).

## Validation chain (per commit)

Pick a routine feature subset that pulls in every magnetar facet
EXCEPT two opt-in cells:

- `crypto-fips` — needs a FIPS native build toolchain;
  `cargo run -p xtask -- check-crypto-matrix` covers it exhaustively in CI.
- `auth-sasl-kerberos` — needs `libkrb5-dev` + `libclang-dev` for
  `libgssapi-sys`; covered by the `e2e_sasl_kerberos.rs` Docker e2e
  per [ADR-0029](../specs/adr/0029-sasl-kerberos-gssapi-scope.md).

```
FEATURES="tokio,moonpool,admin,auth-oauth2,auth-sasl,auth-athenz,auth-athenz-zts,encryption,experimental-v5-client,scalable-topics,crypto-aws-lc-rs"

cargo +nightly fmt --all
cargo build --workspace --no-default-features --features "$FEATURES"
cargo clippy --workspace --no-default-features --features "$FEATURES" --all-targets -- -D warnings
cargo test --workspace --no-default-features --features "$FEATURES" --locked
cargo deny check
RUSTDOCFLAGS="-D warnings --cfg tokio_unstable --cfg tracing_unstable" \
  cargo doc --workspace --no-default-features --features "$FEATURES" --no-deps --locked
cargo run -p xtask -- check-no-channels         # ADR-0003
cargo run -p xtask -- check-no-io-deps          # ADR-0004
cargo run -p xtask -- check-no-internal-clock   # ADR-0011
cargo run -p xtask -- codegen --check
cargo run -p xtask -- check-sim-coverage        # ADR-0024 patch coverage
cargo run -p xtask -- check-runtime-test-parity # ADR-0024 1:1 runtime parity
cargo run -p xtask -- check-crypto-matrix       # ADR-0035 per-provider build matrix incl. FIPS
```

Contributors with a FIPS toolchain locally can substitute
`--all-features` for `--no-default-features --features "$FEATURES"`.
Per-package invocations (`-p magnetar-runtime-tokio --features X`)
need an explicit crypto feature because dependency features don't
transitively activate under `-p`. See
[ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md) for the
provider matrix and `cargo run -p xtask -- check-crypto-matrix` for the
authoritative per-provider build sweep.

Per ADR-0045, the suite spins `apachepulsar/pulsar:4.0.4`
in Docker and exercises the public surface against a real broker — see
[`testing.md`](testing.md).
