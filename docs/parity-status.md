# Java Parity Status

The authoritative parity matrix lives in
[`../README.md#java-client-parity-matrix`](../README.md#java-client-parity-matrix).
This document gives the matrix its engine-by-engine cut and lists the
genuine deferred-scope items.

Per [ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md),
the Java parity matrix is satisfied **by the tokio engine**. The
moonpool engine reaches feature parity with tokio on its own
follow-up train; the gap is tracked below.

## Engine surface

| Surface | tokio | moonpool |
| --- | --- | --- |
| Engine driver loop + transport scaffold | ✅ | ✅ |
| Client (lookup + partitioned-metadata + topic-watch) | ✅ | ✅ |
| Producer façade (send / flush / close) | ✅ | ✅ |
| Consumer façade (subscribe / receive / ack) | ✅ | ✅ |
| Supervised reconnect (Stage 2 + Stage 3 rebuild) | ✅ | ✅ |
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
| Deterministic chaos pack | n/a | ✅ |
| tokio ↔ moonpool differential equivalence harness | n/a | ✅ |

`MoonpoolEngine<P>` is generic over the
[`moonpool_core::Providers`](https://crates.io/crates/moonpool-core)
bundle. `TokioProviders` runs it against a real broker;
`moonpool-sim`'s `SimProviders` runs it under deterministic seeds
([`moonpool-engine.md`](moonpool-engine.md)).

**Six of seven dependent surfaces fully lifted** per ADR-0026 §D1
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
remaining gap is at the **entry-point** level on `PulsarClient<E>`
itself — `partitioned_producer`, `table_view`, and
`typed_table_view` still live in `impl PulsarClient<TokioEngine>`
rather than the engine-generic block (see
[`follow-ups.md` §Per-surface builder + impl-body lifts](follow-ups.md#per-surface-builder--impl-body-lifts)).

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

## Genuine deferred-scope items

Everything else with a `🟡` or `❌` in the README parity matrix is one of:

| Item | Status | Why deferred |
| --- | --- | --- |
| **SASL `PLAIN` (RFC 4616)** | ✅ landed | `magnetar_auth_sasl::SaslPlain` emits the `\0<user>\0<pass>` payload. Useful for username/password broker auth in tests and for brokers configured without a token provider. |
| **SASL Kerberos / GSSAPI** | ✅ landed | `magnetar_auth_sasl::SaslKerberos` binds `libgssapi` under the `auth-sasl-kerberos` façade feature. The multi-round `AUTH_CHALLENGE` continuation threads through `AuthProvider::respond_to_challenge`. The four sans-io test layers per [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md) drive a `ScriptedGssapiClient`; e2e uses a Dockerised KDC. See [ADR-0029](../specs/adr/0029-sasl-kerberos-gssapi-scope.md). |
| **Athenz (pre-fetched role token)** | ✅ landed | `AthenzProvider::with_role_token` uses the supplied token as the `auth_data` payload — useful when an out-of-band agent (`zts-agent`, sidecar) already mints the token. |
| **Athenz (ZTS round-trip)** | 🟡 deferred to v0.2.0 | `AthenzProvider::new(...).initial` returns `AuthError::Unsupported`; the ZTS/ZMS client is deferred per [ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) §D3. |
| **PIP-460** — Scalable topics | ❌ | Experimental in Apache Pulsar; surface still iterating upstream. v0.2.0. |
| **PIP-466** — V5 client surface | ❌ | Inspired by, not adopted verbatim; magnetar already follows the spirit. v0.2.0 if verbatim adoption is desired. |
| **PIP-180** — Shadow topic | ✅ landed | v0.2.0 ([ADR-0033](../specs/adr/0033-pip-180-shadow-topic-scope.md), [`docs/shadow-topic.md`](shadow-topic.md)). Three admin REST methods (`create_shadow_topic` / `delete_shadow_topic` / `get_shadow_topics` + `get_shadow_source`), producer-side `send_with_source_message_id` propagating `CommandSend.message_id`, consumer-side `MessageReceivedFromShadow` event, structural `MessageId` equality across source ⇄ shadow. No proto bump. Caveat: source `MessageId` is client-asserted (broker validates write auth but does not prove the id is from a real source entry — upstream behaviour). |
| **PIP-33** — Replicated subscriptions | ✅ landed (v0.2.0) | `ConsumerBuilder::replicate_subscription_state(bool)` flips `CommandSubscribe` field 14; receive-path filter drops `REPLICATED_SUBSCRIPTION_*` markers and emits `ConnectionEvent::ReplicatedSubscriptionMarkerObserved`. Snapshot generation stays broker-side per [ADR-0034](../specs/adr/0034-pip-33-replicated-subscriptions-scope.md). Two-cluster e2e runs weekly via [`.github/workflows/e2e-replicated-subs.yml`](../.github/workflows/e2e-replicated-subs.yml). Docs: [`docs/replicated-subscriptions.md`](replicated-subscriptions.md). |

These are not required for v0.1.0 under
[ADR-0010](../specs/adr/0010-v0-1-full-java-parity.md), which v0.1.0
satisfies on the tokio engine.

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
  `cargo xtask check-crypto-matrix` covers it exhaustively in CI.
- `auth-sasl-kerberos` — needs `libkrb5-dev` + `libclang-dev` for
  `libgssapi-sys`; covered by the `e2e_sasl_kerberos.rs` Docker e2e
  per [ADR-0029](../specs/adr/0029-sasl-kerberos-gssapi-scope.md).

```
FEATURES="tokio,moonpool,admin,auth-oauth2,auth-sasl,auth-athenz,encryption,crypto-aws-lc-rs"

cargo +nightly fmt --all
cargo build --workspace --no-default-features --features "$FEATURES"
cargo clippy --workspace --no-default-features --features "$FEATURES" --all-targets -- -D warnings
cargo test --workspace --no-default-features --features "$FEATURES" --locked
cargo deny check
RUSTDOCFLAGS="-D warnings --cfg tokio_unstable" \
  cargo doc --workspace --no-default-features --features "$FEATURES" --no-deps --locked
cargo xtask check-no-channels         # ADR-0003
cargo xtask check-no-io-deps          # ADR-0004
cargo xtask check-no-internal-clock   # ADR-0011
cargo xtask codegen --check
cargo xtask check-sim-coverage        # ADR-0024 patch coverage
cargo xtask check-runtime-test-parity # ADR-0024 1:1 runtime parity
cargo xtask check-crypto-matrix       # ADR-0035 per-provider build matrix incl. FIPS
```

Contributors with a FIPS toolchain locally can substitute
`--all-features` for `--no-default-features --features "$FEATURES"`.
Per-package invocations (`-p magnetar-runtime-tokio --features X`)
need an explicit crypto feature because dependency features don't
transitively activate under `-p`. See
[ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md) for the
provider matrix and `cargo xtask check-crypto-matrix` for the
authoritative per-provider build sweep.

Behind `--features e2e`, the suite spins `apachepulsar/pulsar:4.0.4`
in Docker and exercises the public surface against a real broker — see
[`testing.md`](testing.md).
