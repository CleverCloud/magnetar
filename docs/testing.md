# Testing

Magnetar's test surface has five categories. Each is a normal
`cargo test` target — the difference is which dependencies it pulls in
and whether the target is gated behind a feature flag or `#[ignore]`.

## Categories

| Category | Where | Gating | Needs | Default-on |
| --- | --- | --- | --- | --- |
| **Unit** | `crates/<crate>/src/**` in `#[cfg(test)] mod tests` blocks | none | nothing | yes |
| **Integration** | `crates/<crate>/tests/*.rs` | none | nothing | yes |
| **Deterministic chaos** | [`crates/magnetar-runtime-moonpool/tests/`](../crates/magnetar-runtime-moonpool/tests/) | `--features crypto-aws-lc-rs` (or any single `crypto-*` provider — per-package `--all-features` would pull `crypto-fips` and its native toolchain) | nothing (virtual everything) | yes |
| **Differential equivalence** | [`crates/magnetar-differential/tests/`](../crates/magnetar-differential/tests/) | When run with `--workspace`, use the routine feature subset (see [Running each category](#running-each-category)); when run standalone (`-p magnetar-differential`), forward a crypto provider feature to the runtime deps | nothing | yes |
| **End-to-end (e2e)** | [`crates/magnetar/tests/e2e_*.rs`](../crates/magnetar/tests/) | none (ADR-0046 — runs as a regular `cargo test`) | Docker + `apachepulsar/pulsar:4.0.4` (host or CI runner must have it) | yes |

## Running each category

```bash
# Routine feature subset that activates every magnetar facet EXCEPT:
# - `crypto-fips` (native FIPS toolchain isn't universally available);
# - `auth-sasl-kerberos` (needs `libkrb5-dev` + `libclang-dev` for
#   `libgssapi-sys`).
# `cargo run -p xtask -- check-crypto-matrix` covers FIPS exhaustively in CI;
# the GSSAPI provider is exercised by the `e2e_sasl_kerberos.rs`
# Docker e2e test (see [ADR-0029](../specs/adr/0029-sasl-kerberos-gssapi-scope.md)).
FEATURES="tokio,moonpool,admin,auth-oauth2,auth-sasl,auth-athenz,auth-athenz-zts,encryption,experimental-v5-client,scalable-topics,crypto-aws-lc-rs"

# Unit + integration (no broker, no Docker).
cargo test --workspace --no-default-features --features "$FEATURES" --locked

# Moonpool deterministic-simulation suite (single seed; default).
# Per-package `--all-features` would activate `crypto-fips` and need
# a native FIPS toolchain — use a single provider feature instead.
cargo test -p magnetar-runtime-moonpool --features crypto-aws-lc-rs --locked

# Same, swept across seeds 1..32 (local pre-flight; CI runs a 16-random-seed
# sweep daily — see .github/workflows/moonpool-seed-sweep.yml / ADR-0036).
for seed in $(seq 1 32); do
  MOONPOOL_SEED=$seed cargo test -p magnetar-runtime-moonpool \
    --features crypto-aws-lc-rs --locked -- --quiet || echo "seed $seed FAILED"
done

# Differential equivalence harness. The crate has no crypto features
# of its own, so `-p magnetar-differential --all-features` activates
# nothing on the runtime deps and the cfg cascade fires. Either run
# it as part of `--workspace --features "$FEATURES"` above, or
# forward a crypto provider feature explicitly to the runtime deps:
cargo test -p magnetar-differential --locked --features \
  'magnetar-runtime-tokio/crypto-aws-lc-rs,magnetar-runtime-moonpool/crypto-aws-lc-rs'

# End-to-end suite (Docker required, runs apachepulsar/pulsar:4.0.4).
# Per ADR-0046 the e2e suite is **already part of** the `--workspace`
# invocations above when `--all-features` is on — no `--features e2e`,
# no `--include-ignored`. The line below is the bare-minimum invocation
# that exercises only the e2e tests:
cargo test -p magnetar --tests
```

Contributors with a FIPS toolchain installed locally can substitute
`--all-features` for `--no-default-features --features "$FEATURES"`
above. `cargo run -p xtask -- check-crypto-matrix` is the authoritative
per-provider sweep regardless.

The validation chain documented in
[`parity-status.md#validation-chain-per-commit`](parity-status.md#validation-chain-per-commit)
runs everything **including the e2e suite** (ADR-0046 folded the
former opt-in `e2e` job into the regular `test` job).

## Unit tests

`magnetar-proto` ships 270+ unit tests that exercise sans-io behavior
in isolation: feed bytes in, assert events / transmit / state. Every
protocol bug is reproducible without sockets or async tasks. Ported
behavioral cases include:

- 13 ack-grouping + unacked-tracker cases from Java's
  `AckGroupingTrackerTest` + `UnAckedMessageTrackerTest`.
- 6 batch-container cases from Java's `BatchMessageContainerImplTest`.
- ~14 schema codec cases.
- 8 PIP-180 shadow-topic cases (3 producer encode-site guards including
  a wire-byte-identity regression test for the no-source-id default,
  1 `MessageId` structural equality pin, 4 consumer-side classification
  cases).
- 11 PIP-33 marker-decoder + filter cases.

### Four-layer PIP coverage (ADR-0024)

Every PIP-bearing change lands as the full
[ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md)
test set in the same commit. PIP-180 is a worked example:

| Layer | File |
| --- | --- |
| (a) `magnetar-proto` unit | [`crates/magnetar-proto/src/{producer,consumer,types}.rs`](../crates/magnetar-proto/src/) `#[cfg(test)] mod tests` |
| (b) `magnetar-runtime-tokio` integration | [`crates/magnetar-runtime-tokio/tests/shadow_topic.rs`](../crates/magnetar-runtime-tokio/tests/shadow_topic.rs) |
| (c) `magnetar-runtime-moonpool` integration | [`crates/magnetar-runtime-moonpool/tests/shadow_topic.rs`](../crates/magnetar-runtime-moonpool/tests/shadow_topic.rs) |
| (d) `magnetar-differential` equivalence | [`crates/magnetar-differential/tests/shadow_topic_equivalence.rs`](../crates/magnetar-differential/tests/shadow_topic_equivalence.rs) + golden trace [`tests/golden/shadow_send_with_source.json`](../crates/magnetar-differential/tests/golden/shadow_send_with_source.json) |
| (admin REST) `magnetar-admin` wiremock | [`crates/magnetar-admin/tests/pip_180_shadow_topic.rs`](../crates/magnetar-admin/tests/pip_180_shadow_topic.rs) |
| (e2e) Docker against `apachepulsar/pulsar:4.0.4` | [`crates/magnetar/tests/e2e_shadow_topic.rs`](../crates/magnetar/tests/e2e_shadow_topic.rs) |

## Integration tests

`crates/<crate>/tests/*.rs` covers what unit tests cannot — the engine
glue (`magnetar-runtime-tokio`, `magnetar-runtime-moonpool`), the
façade builders (`magnetar`), the auth crates. No external services
required; everything stays in-process.

## Deterministic chaos pack

Lives in
[`crates/magnetar-runtime-moonpool/tests/`](../crates/magnetar-runtime-moonpool/tests/).
Targets the supervised reconnect path, the PIP-121 +
PIP-188 reconnection flows, virtual-clock timers, and OAuth2 token
refresh edges. The supervised reconnect body (anti-thrash cooldown +
multi-attempt redial) is exercised by
[`supervised_redial.rs`](../crates/magnetar-runtime-moonpool/tests/supervised_redial.rs)
— a `SimProviders` drop → accept → drop → accept fixture paired 1:1 with
the real-loopback tokio mirror
[`crates/magnetar-runtime-tokio/tests/supervised_redial.rs`](../crates/magnetar-runtime-tokio/tests/supervised_redial.rs).
See
[`moonpool-engine.md#deterministic-chaos-pack`](moonpool-engine.md#deterministic-chaos-pack)
for the per-scenario breakdown.

## Differential equivalence

Lives in
[`crates/magnetar-differential/tests/`](../crates/magnetar-differential/tests/).
Runs a `Trace` against both `magnetar-runtime-tokio` and
`magnetar-runtime-moonpool` and asserts user-visible `EventStream`
equivalence. See
[`moonpool-engine.md#differential-equivalence-harness`](moonpool-engine.md#differential-equivalence-harness).
Notable equivalence suites:

| File | Coverage |
| --- | --- |
| [`crypto_roundtrip_equivalence.rs`](../crates/magnetar-differential/tests/crypto_roundtrip_equivalence.rs) | PIP-4 encrypted round-trip parity across both engines ([ADR-0044](../specs/adr/0044-moonpool-message-crypto-bridge.md)). |
| [`crypto_failure_action_equivalence.rs`](../crates/magnetar-differential/tests/crypto_failure_action_equivalence.rs) | The 3-arm `cryptoFailureAction` matrix (Fail / Discard / Consume), pinned by golden trace [`golden/crypto_failure_action.json`](../crates/magnetar-differential/tests/golden/crypto_failure_action.json). |

## End-to-end (Docker)

Every `crates/magnetar/tests/e2e_*.rs` file is gated on
`#[cfg(feature = "e2e")]` AND `#[ignore = "e2e: requires Docker"]`.
Both gates have to be cleared for the test to run, by design:

Per [ADR-0046](../specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md)
the e2e suite carries **no feature flag and no `#[ignore]`** — every
`cargo test` invocation that activates the workspace runs the e2e
tests. Contributors without Docker on the host should run unit /
integration / moonpool tests crate-by-crate (`-p magnetar-proto`,
`-p magnetar-runtime-tokio`, `-p magnetar-runtime-moonpool`,
`-p magnetar-differential`) which never touch the network boundary.

```bash
# Full validation chain (runs e2e automatically when Docker is present).
cargo test --workspace --all-features --locked
```

Suites cover:

| File | Coverage |
| --- | --- |
| [`e2e_pulsar.rs`](../crates/magnetar/tests/e2e_pulsar.rs) | Basic producer + consumer round-trip. |
| [`e2e_schemas.rs`](../crates/magnetar/tests/e2e_schemas.rs) | Bytes / String / JSON / Int32 schemas. |
| [`e2e_schemas_extended.rs`](../crates/magnetar/tests/e2e_schemas_extended.rs) | Avro, Protobuf, KeyValue, ProtobufNative. |
| [`e2e_dlq.rs`](../crates/magnetar/tests/e2e_dlq.rs) | DLQ + `reconsume_later`. |
| [`e2e_batch_chunk.rs`](../crates/magnetar/tests/e2e_batch_chunk.rs) | Batching + PIP-37 chunking. |
| [`e2e_interceptors_ack.rs`](../crates/magnetar/tests/e2e_interceptors_ack.rs) | Interceptor SPIs + ack patterns. |
| [`e2e_transactions.rs`](../crates/magnetar/tests/e2e_transactions.rs) | PIP-31 commit / abort. |
| [`e2e_sub_types.rs`](../crates/magnetar/tests/e2e_sub_types.rs) | Shared / Failover / Key_Shared. |
| [`e2e_partitioned_deep.rs`](../crates/magnetar/tests/e2e_partitioned_deep.rs) | Partitioned producer + consumer. |
| [`e2e_compacted.rs`](../crates/magnetar/tests/e2e_compacted.rs) | Compacted topics + TableView (PIP-94). |
| [`e2e_persistence.rs`](../crates/magnetar/tests/e2e_persistence.rs) | Persistent + non-persistent semantics. |
| [`e2e_crypto.rs`](../crates/magnetar/tests/e2e_crypto.rs) | PIP-4 + `cryptoFailureAction` (Fail / Discard / Consume). |
| [`e2e_oauth2.rs`](../crates/magnetar/tests/e2e_oauth2.rs) | OAuth2 `ClientCredentialsFlow` + token cache + refresh-on-expiry. |
| [`e2e_sasl_kerberos.rs`](../crates/magnetar/tests/e2e_sasl_kerberos.rs) | SASL Kerberos / GSSAPI via `libgssapi` against a Dockerised MIT KDC (`gcavalcante8808/krb5-server`). Gated on `--features auth-sasl-kerberos`; needs `libkrb5-dev` + `libclang-dev` on the build host. See [ADR-0029](../specs/adr/0029-sasl-kerberos-gssapi-scope.md). |
| [`e2e_dns_resolver.rs`](../crates/magnetar/tests/e2e_dns_resolver.rs) | Custom `DnsResolver` plumbed end-to-end. |
| [`e2e_force_unsubscribe.rs`](../crates/magnetar/tests/e2e_force_unsubscribe.rs) | PIP-313 force unsubscribe. |
| [`e2e_memory_limit.rs`](../crates/magnetar/tests/e2e_memory_limit.rs) | `MemoryLimitPolicy::{FailImmediately, ProducerBlock}`. |
| [`e2e_pattern_auto_reconcile.rs`](../crates/magnetar/tests/e2e_pattern_auto_reconcile.rs) | PIP-145 background-ticker rediscovery. |
| [`e2e_reconnect.rs`](../crates/magnetar/tests/e2e_reconnect.rs) | Supervised reconnect under broker stop/start. |
| [`e2e_rolling_stats.rs`](../crates/magnetar/tests/e2e_rolling_stats.rs) | Rolling-window stats (msgs/sec, bytes/sec, latency p50/p99/max). |
| [`e2e_seek_per_partition.rs`](../crates/magnetar/tests/e2e_seek_per_partition.rs) | Per-partition seek callbacks. |
| [`e2e_cluster_failover.rs`](../crates/magnetar/tests/e2e_cluster_failover.rs) | PIP-121 manual cluster swap with two broker containers. |
| [`e2e_shadow_topic.rs`](../crates/magnetar/tests/e2e_shadow_topic.rs) | PIP-180 — admin REST shadow-topic management, `send_with_source_message_id` propagation, `MessageReceivedFromShadow` consumer event. |
| [`e2e_replicated_subscriptions.rs`](../crates/magnetar/tests/e2e_replicated_subscriptions.rs) | PIP-33 cursor-resume across two clusters. Runs on every PR per [ADR-0046](../specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md). The `test` job in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) brings up the two-cluster docker-compose fixture (`fixtures/docker-compose.replicated-subs.yml`) before `cargo test`. |

## The `#[ignore]` policy

Per [ADR-0021](../specs/adr/0021-no-silent-test-ignore-or-remove.md):
`#[ignore]` is reserved for environment dependencies the build host
cannot satisfy. Every `#[ignore]` annotation must:

1. Carry a reason string (`#[ignore = "e2e: requires Docker"]`,
   `#[ignore = "m8-followup: …"]`).
2. Either gate on an actual environment requirement (Docker, network),
   **or** link to a tracked follow-up in
   [`follow-ups.md`](follow-ups.md).

Bug-hiders are not acceptable. If a test fails, fix the underlying
defect or remove the test with a written rationale; do not paper over
it with a silent `#[ignore]`.

## Mutation testing (scoped)

```bash
cargo mutants --package magnetar-proto --timeout 60 --shard 1/4
```

Targets frame decode, request correlation, resend/dedup, flow permits,
chunk metadata, timeout transitions. Time-boxed and run nightly +
`workflow_dispatch`.

## Fuzz

```bash
cargo +nightly fuzz run encode_roundtrip
```

Round-trip-encodes `BaseCommand` shapes and asserts re-decode
equality. Lives in
[`crates/magnetar-proto/fuzz/`](../crates/magnetar-proto/fuzz/).
Requires nightly; orthogonal to the moonpool engine.
