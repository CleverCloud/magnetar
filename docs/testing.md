# Testing

Magnetar's test surface has five categories. Each is a normal
`cargo test` target — the difference is which dependencies it pulls in
and whether the target is gated behind a feature flag or `#[ignore]`.

## Categories

| Category | Where | Gating | Needs | Default-on |
| --- | --- | --- | --- | --- |
| **Unit** | `crates/<crate>/src/**` in `#[cfg(test)] mod tests` blocks | none | nothing | yes |
| **Integration** | `crates/<crate>/tests/*.rs` | none | nothing | yes |
| **Deterministic chaos** | [`crates/magnetar-runtime-moonpool/tests/`](../crates/magnetar-runtime-moonpool/tests/) | `--all-features` | nothing (virtual everything) | yes |
| **Differential equivalence** | [`crates/magnetar-differential/tests/`](../crates/magnetar-differential/tests/) | `--all-features` | nothing | yes |
| **End-to-end (e2e)** | [`crates/magnetar/tests/e2e_*.rs`](../crates/magnetar/tests/) | `--features e2e` + `#[ignore = "e2e: requires Docker"]` | Docker + `apachepulsar/pulsar:4.0.4` | no |

## Running each category

```bash
# Unit + integration (no broker, no Docker).
cargo test --workspace --all-features --locked

# Moonpool deterministic-simulation suite (single seed; default).
cargo test -p magnetar-runtime-moonpool --all-features --locked

# Same, swept across seeds 1..32.
for seed in $(seq 1 32); do
  MOONPOOL_SEED=$seed cargo test -p magnetar-runtime-moonpool \
    --all-features --locked -- --quiet || echo "seed $seed FAILED"
done

# Differential equivalence harness.
cargo test -p magnetar-differential --all-features --locked

# End-to-end suite (Docker required, runs apachepulsar/pulsar:4.0.4).
cargo test --workspace --features e2e -- --include-ignored
```

The validation chain documented in
[`parity-status.md#validation-chain-per-commit`](parity-status.md#validation-chain-per-commit)
runs everything except the e2e suite (e2e is opt-in for both local
runs and CI).

## Unit tests

`magnetar-proto` ships 220+ unit tests that exercise sans-io behavior
in isolation: feed bytes in, assert events / transmit / state. Every
protocol bug is reproducible without sockets or async tasks. Ported
behavioral cases include:

- 13 ack-grouping + unacked-tracker cases from Java's
  `AckGroupingTrackerTest` + `UnAckedMessageTrackerTest`.
- 6 batch-container cases from Java's `BatchMessageContainerImplTest`.
- ~14 schema codec cases.

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
refresh edges. See
[`moonpool-engine.md#deterministic-chaos-pack`](moonpool-engine.md#deterministic-chaos-pack)
for the per-scenario breakdown.

## Differential equivalence

Lives in
[`crates/magnetar-differential/tests/`](../crates/magnetar-differential/tests/).
Runs a `Trace` against both `magnetar-runtime-tokio` and
`magnetar-runtime-moonpool` and asserts user-visible `EventStream`
equivalence. See
[`moonpool-engine.md#differential-equivalence-harness`](moonpool-engine.md#differential-equivalence-harness).

## End-to-end (Docker)

Every `crates/magnetar/tests/e2e_*.rs` file is gated on
`#[cfg(feature = "e2e")]` AND `#[ignore = "e2e: requires Docker"]`.
Both gates have to be cleared for the test to run, by design:

- The `e2e` feature pulls in `testcontainers` + the `apachepulsar/pulsar:4.0.4`
  image + the auth crates as needed.
- The `#[ignore]` prevents the suite from running in environments
  without Docker (most contributor laptops + the no-Docker CI shards).

To run the suite, both flags must be set:

```bash
cargo test --workspace --features e2e -- --include-ignored
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
| [`e2e_dns_resolver.rs`](../crates/magnetar/tests/e2e_dns_resolver.rs) | Custom `DnsResolver` plumbed end-to-end. |
| [`e2e_force_unsubscribe.rs`](../crates/magnetar/tests/e2e_force_unsubscribe.rs) | PIP-313 force unsubscribe. |
| [`e2e_memory_limit.rs`](../crates/magnetar/tests/e2e_memory_limit.rs) | `MemoryLimitPolicy::{FailImmediately, ProducerBlock}`. |
| [`e2e_pattern_auto_reconcile.rs`](../crates/magnetar/tests/e2e_pattern_auto_reconcile.rs) | PIP-145 background-ticker rediscovery. |
| [`e2e_reconnect.rs`](../crates/magnetar/tests/e2e_reconnect.rs) | Supervised reconnect under broker stop/start. |
| [`e2e_rolling_stats.rs`](../crates/magnetar/tests/e2e_rolling_stats.rs) | Rolling-window stats (msgs/sec, bytes/sec, latency p50/p99/max). |
| [`e2e_seek_per_partition.rs`](../crates/magnetar/tests/e2e_seek_per_partition.rs) | Per-partition seek callbacks. |
| [`e2e_cluster_failover.rs`](../crates/magnetar/tests/e2e_cluster_failover.rs) | PIP-121 manual cluster swap with two broker containers. |

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
