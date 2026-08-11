# Testing

Magnetar's test surface has five categories.
Each is a normal `cargo test` target — the difference is which dependencies it pulls in and whether the target is gated behind a feature flag or `#[ignore]`.

## Categories

| Category                     | Where                                                                                   | Gating                                                                                                                                                                                                                     | Needs                                                                | Default-on                       |
| ---------------------------- | --------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- | -------------------------------- |
| **Unit**                     | `crates/<crate>/src/**` in `#[cfg(test)] mod tests` blocks                              | none                                                                                                                                                                                                                       | nothing                                                              | yes                              |
| **Integration**              | `crates/<crate>/tests/*.rs`                                                             | none                                                                                                                                                                                                                       | nothing                                                              | yes                              |
| **Deterministic chaos**      | [`crates/magnetar-runtime-moonpool/tests/`](../crates/magnetar-runtime-moonpool/tests/) | `--no-default-features --features crypto-aws-lc-rs` (or another single `crypto-*` provider — per-package `--all-features` also pulls `crypto-fips`)                                                                        | nothing external; `SimProviders` virtualises executor, time, and I/O | yes                              |
| **Differential equivalence** | [`crates/magnetar-differential/tests/`](../crates/magnetar-differential/tests/)         | When run with `--workspace`, use the routine feature subset (see [Running each category](#running-each-category)); when run standalone (`-p magnetar-differential`), forward a crypto provider feature to the runtime deps | nothing                                                              | yes                              |
| **End-to-end (e2e)**         | [`crates/magnetar/tests/e2e_*.rs`](../crates/magnetar/tests/)                           | no dedicated `e2e` feature or `#[ignore]`; owning product features still apply (for example, `scalable-topics`)                                                                                                            | Docker + each suite's pinned Pulsar 4.x or 5.0.0-M1 image            | yes for default product surfaces |

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

# Workspace unit + integration plus every regular e2e target activated by
# this product-feature set. Docker is required; run the crate-by-crate commands
# documented below when only broker-free tests are wanted.
cargo test --workspace --no-default-features --features "$FEATURES" --locked

# Moonpool deterministic-simulation suite (single seed; default).
# Per-package `--all-features` would activate `crypto-fips` and need
# a native FIPS toolchain — use a single provider feature instead.
cargo test -p magnetar-runtime-moonpool \
  --no-default-features --features crypto-aws-lc-rs --locked

# Same, swept across seeds 1..32 (local pre-flight; CI runs a 128-random-seed
# sweep daily — see .github/workflows/moonpool-seed-sweep.yml / ADR-0036).
for seed in $(seq 1 32); do
  MOONPOOL_SEED=$seed cargo test -p magnetar-runtime-moonpool \
    --no-default-features --features crypto-aws-lc-rs \
    --locked -- --quiet || { echo "seed $seed FAILED"; exit 1; }
done

# Differential equivalence harness. The crate has no crypto features
# of its own, so `-p magnetar-differential --all-features` activates
# nothing on the runtime deps and the cfg cascade fires. Either run
# it as part of `--workspace --features "$FEATURES"` above, or
# forward a crypto provider feature explicitly to the runtime deps:
cargo test -p magnetar-differential --locked --features \
  'magnetar-runtime-tokio/crypto-aws-lc-rs,magnetar-runtime-moonpool/crypto-aws-lc-rs'

# End-to-end suite (Docker required; each target selects its broker image).
# Per ADR-0046 there is no dedicated `e2e` feature and no ignored suite.
# Product feature gates still apply: --all-features activates all of them.
# This first command exercises the default-product e2e targets:
cargo test -p magnetar-driver --tests

# The PIP-460 target is behind its owning default-off product feature:
cargo build -p magnetarctl --features scalable-topics
cargo test -p magnetar-driver --features scalable-topics \
  --test e2e_scalable_topic
```

Contributors with a FIPS toolchain installed locally can substitute `--all-features` for `--no-default-features --features "$FEATURES"` above.
`cargo run -p xtask -- check-crypto-matrix` is the authoritative per-provider sweep regardless.

The validation chain documented in [`../CONTRIBUTING.md#validation-chain`](../CONTRIBUTING.md#validation-chain) runs everything **including the e2e suite** in one local command.
Per ADR-0098, per-PR CI executes the same surface as one non-e2e matrix cell and four e2e cells so they run concurrently and each stays below the 180-minute ceiling.
The e2e cells derive their inventory from the sorted `e2e_*.rs` filenames, and only the cell containing `e2e_replicated_subscriptions` starts the PIP-33 fixture.
The cell containing `e2e_scalable_topic` builds `magnetarctl` before that target because its CLI round-trip test consumes the companion binary and target-specific Cargo test commands do not build other workspace packages.

## Unit tests

`magnetar-proto` ships 270+ unit tests that exercise sans-io behavior in isolation: feed bytes in, assert events / transmit / state.
Every protocol bug is reproducible without sockets or async tasks.
Ported behavioral cases include:

- 13 ack-grouping + unacked-tracker cases from Java's `AckGroupingTrackerTest` + `UnAckedMessageTrackerTest`.
- 6 batch-container cases from Java's `BatchMessageContainerImplTest`.
- ~14 schema codec cases.
- 8 PIP-180 shadow-topic cases (3 producer encode-site guards including a wire-byte-identity regression test for the no-source-id default, 1 `MessageId` structural equality pin, 4 consumer-side classification cases).
- 11 PIP-33 marker-decoder + filter cases.

### Four-layer PIP coverage (ADR-0024)

Every PIP-bearing change lands as the full [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md) test set in the same commit.
PIP-180 is a worked example:

| Layer                                            | File                                                                                                                                                                                                                                                                          |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| (a) `magnetar-proto` unit                        | [`crates/magnetar-proto/src/{producer,consumer,types}.rs`](../crates/magnetar-proto/src/) `#[cfg(test)] mod tests`                                                                                                                                                            |
| (b) `magnetar-runtime-tokio` integration         | [`crates/magnetar-runtime-tokio/tests/shadow_topic.rs`](../crates/magnetar-runtime-tokio/tests/shadow_topic.rs)                                                                                                                                                               |
| (c) `magnetar-runtime-moonpool` integration      | [`crates/magnetar-runtime-moonpool/tests/shadow_topic.rs`](../crates/magnetar-runtime-moonpool/tests/shadow_topic.rs)                                                                                                                                                         |
| (d) `magnetar-differential` equivalence          | [`crates/magnetar-differential/tests/shadow_topic_equivalence.rs`](../crates/magnetar-differential/tests/shadow_topic_equivalence.rs) + golden trace [`tests/golden/shadow_send_with_source.json`](../crates/magnetar-differential/tests/golden/shadow_send_with_source.json) |
| (admin REST) `magnetar-admin` wiremock           | [`crates/magnetar-admin/tests/pip_180_shadow_topic.rs`](../crates/magnetar-admin/tests/pip_180_shadow_topic.rs)                                                                                                                                                               |
| (e2e) Docker against `apachepulsar/pulsar:4.0.4` | [`crates/magnetar/tests/e2e_shadow_topic.rs`](../crates/magnetar/tests/e2e_shadow_topic.rs)                                                                                                                                                                                   |

## Integration tests

`crates/<crate>/tests/*.rs` covers what unit tests cannot — the engine glue (`magnetar-runtime-tokio`, `magnetar-runtime-moonpool`), the façade builders (`magnetar`), the auth crates.
No external services required; everything stays in-process.

## Deterministic chaos pack

Lives in [`crates/magnetar-runtime-moonpool/tests/`](../crates/magnetar-runtime-moonpool/tests/).
The `SimProviders` suites run on Moonpool 0.8's native seeded executor without an ambient Tokio runtime.
The scalable-topic target runs the complete aggregate over simulated controller and segment sockets for four schedules derived from `MOONPOOL_SEED`, including two-source delivery, status, position, acknowledgement, and close.
They route tasks, time, network I/O, and concurrent selection through the provider boundary, and `sim_chaos.rs` asserts invariants over named flat `tracing` events queried through `TraceQuery`.
The pack targets the supervised reconnect path, PIP-121 + PIP-188 reconnection flows, virtual-clock timers, and OAuth2 token refresh edges.
The supervised reconnect body (anti-thrash cooldown + multi-attempt redial) is exercised by [`supervised_redial.rs`](../crates/magnetar-runtime-moonpool/tests/supervised_redial.rs) — a `SimProviders` drop → accept → drop → accept fixture paired 1:1 with the real-loopback tokio mirror [`crates/magnetar-runtime-tokio/tests/supervised_redial.rs`](../crates/magnetar-runtime-tokio/tests/supervised_redial.rs).
See [`moonpool-engine.md#deterministic-chaos-pack`](moonpool-engine.md#deterministic-chaos-pack) for the per-scenario breakdown.

### Swarm configurations (ADR-0097)

Each `sim_chaos.rs` produce/consume iteration derives a `SwarmConfig` from its seed — a pure splitmix64 hash over `moonpool_sim::current_sim_seed()` that consumes no RNG stream — and runs a subset of the optional campaign features: the four [ADR-0048](../specs/adr/0048-buggify-fault-injection.md) buggify labels (armed per-label through `Buggify::with_rng_and_filter` and the `ConnectionConfig.buggify` engine-arming slot, which only the moonpool engine honours) plus the optional workload operations `op.client_ack` and `op.client_close`, each drawn at 50% inclusion.
1 in 4 seeds runs the inclusive `full()` configuration, an all-off draw collapses to `full()`, and the sub-seed-pinning regression tests force the recorded `baseline()` shape so their trajectories stay byte-identical.
The canonical config line — seed, slot, labels, operations, effective weights, knowingly-vacuous invariants — prints before the workload runs and is embedded in every gate and invariant failure message, so a failing seed reproduces from `MOONPOOL_SEED` alone.
Purity and campaign composition are pinned by [`swarm_config.rs`](../crates/magnetar-runtime-moonpool/tests/swarm_config.rs); the production engine's indifference to the whole surface is pinned by the tokio twin [`swarm_off_is_nop.rs`](../crates/magnetar-runtime-tokio/tests/swarm_off_is_nop.rs).
See [ADR-0097](../specs/adr/0097-swarm-testing-sim-configurations.md).

## Differential equivalence

Lives in [`crates/magnetar-differential/tests/`](../crates/magnetar-differential/tests/).
Runs a `Trace` against both `magnetar-runtime-tokio` and `magnetar-runtime-moonpool` and asserts user-visible `EventStream` equivalence.
See [`moonpool-engine.md#differential-equivalence-harness`](moonpool-engine.md#differential-equivalence-harness).
Notable equivalence suites:

| File                                                                                                                                         | Coverage                                                                                                                                                                                                                                                                                                                                                                   |
| -------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`crypto_roundtrip_equivalence.rs`](../crates/magnetar-differential/tests/crypto_roundtrip_equivalence.rs)                                   | PIP-4 encrypted round-trip parity across both engines ([ADR-0044](../specs/adr/0044-moonpool-message-crypto-bridge.md)).                                                                                                                                                                                                                                                   |
| [`crypto_failure_action_equivalence.rs`](../crates/magnetar-differential/tests/crypto_failure_action_equivalence.rs)                         | The 3-arm `cryptoFailureAction` matrix (Fail / Discard / Consume), pinned by golden trace [`golden/crypto_failure_action.json`](../crates/magnetar-differential/tests/golden/crypto_failure_action.json).                                                                                                                                                                  |
| [`lookup_redirect_chain_equivalence.rs`](../crates/magnetar-differential/tests/lookup_redirect_chain_equivalence.rs)                         | Redirect-target dialing across a multi-broker lookup chain (ADR-0039 amendment).                                                                                                                                                                                                                                                                                           |
| [`message_listener_delivery_equivalence.rs`](../crates/magnetar-differential/tests/message_listener_delivery_equivalence.rs)                 | `MessageListener` push-delivery parity for the single-topic / typed consumer (ADR-0064).                                                                                                                                                                                                                                                                                   |
| [`wrapper_message_listener_delivery_equivalence.rs`](../crates/magnetar-differential/tests/wrapper_message_listener_delivery_equivalence.rs) | `MessageListener` push-delivery parity for the wrapper consumers (multi-topic / partitioned / pattern, ADR-0064).                                                                                                                                                                                                                                                          |
| [`chunk_reassembly_bound_equivalence.rs`](../crates/magnetar-differential/tests/chunk_reassembly_bound_equivalence.rs)                       | Bounded PIP-37 chunk reassembly — cap-eviction parity (ADR-0063).                                                                                                                                                                                                                                                                                                          |
| [`failover_active_reflow_equivalence.rs`](../crates/magnetar-differential/tests/failover_active_reflow_equivalence.rs)                       | Failover active-promotion flow rearming plus accepted incomplete PIP-37 chunk-flow replenishment parity; extended for the `ConsumerEventListener` active-state surface (issue #348, ADR-0081) — `is_active` trajectory and drained `active_changes` transitions across promote / redundant-promote.                                                                        |
| [`nack_unacked_removal_equivalence.rs`](../crates/magnetar-differential/tests/nack_unacked_removal_equivalence.rs)                           | Nacked ids dropped from the ack-timeout tracker (no double redelivery) parity.                                                                                                                                                                                                                                                                                             |
| [`transient_retry_giveup_equivalence.rs`](../crates/magnetar-differential/tests/transient_retry_giveup_equivalence.rs)                       | Configured provisional retry count and exact terminal `ProducerBusy` / `ConsumerBusy` parity for producer-open and subscribe (ADR-0080).                                                                                                                                                                                                                                   |
| [`stream_consumer_equivalence.rs`](../crates/magnetar-differential/tests/stream_consumer_equivalence.rs)                                     | Nine baseline public scalable-aggregate scenarios: typed delivery, concurrent receive and atomic batch, aggregate budget, same-epoch assignment and drain, child reconnect, acknowledgement retry, negative-ack redelivery, close wakeup, and public builder/position/transaction/drop surfaces (ADR-0102).                                                                |
| [`stream_consumer_advanced_equivalence.rs`](../crates/magnetar-differential/tests/stream_consumer_advanced_equivalence.rs)                   | Fourteen advanced public scalable-aggregate scenarios spanning ancestry, exact-M1 sealed-assignment drain without parent reopen, ordering modes, seek/resynchronization, transactions and cancellation, controller ordering/recovery, compressed batch/chunk delivery, child-open and close lifecycle, malformed delivery, and partial acknowledgement failure (ADR-0102). |

## Simulation patch coverage

`cargo run -p xtask -- check-sim-coverage` executes exactly the `magnetar-runtime-moonpool` and `magnetar-differential` test binaries.
It re-exports that one invocation-owned instrumented pass over exactly eight reported and hard-gated packages: `magnetar-proto`, `magnetar-runtime-tokio`, `magnetar-runtime-moonpool`, `magnetar-differential`, `magnetar-auth-athenz`, `magnetar-auth-sasl`, `magnetar-driver` at `crates/magnetar`, and `magnetar-fakes`.
The façade and fake are reached through the differential aggregate tests; façade Docker e2e targets do not execute under this gate.
An added production line in those packages must be reached transitively by one of the two execution roots, while `magnetar-admin`, `magnetarctl`, and other packages those roots do not compile remain advisory `not gated` scope.
A record-less file in one of the eight packages hard-fails when it contains a non-test function body, even if sibling files emitted LCOV records.
Module/export/constant/bodyless-declaration-only files remain advisory because they have no executable coverage mapping.

The original six-package widening measured 63 `SF:` records on 2026-07-31.
ADR-0102 adds the façade and fake without claiming a later record total, and ADR-0100's isolated target, output-only LCOV, artifact rejection, and cleanup behavior remain unchanged.

## End-to-end (Docker)

Per [ADR-0046](../specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) the e2e suite carries **no dedicated `e2e` feature and no `#[ignore]`**. Feature-specific targets still use their owning product feature, so `e2e_scalable_topic.rs` is discovered only with `--features scalable-topics` (or `--all-features`).
Contributors without Docker on the host should run unit / integration / moonpool tests crate-by-crate (`-p magnetar-proto`, `-p magnetar-runtime-tokio`, `-p magnetar-runtime-moonpool`, `-p magnetar-differential`) which never touch the network boundary.
The CI sharding in [ADR-0098](../specs/adr/0098-parallelize-per-pr-test-execution.md) changes execution topology only; it does not make e2e optional or alter the local command.

```bash
# Full validation chain (runs e2e automatically when Docker is present).
cargo test --workspace --all-features --locked
```

The batching and chunking suite defaults to `apachepulsar/pulsar:4.2.3`, the latest stable Pulsar 4.x release on 2026-07-13.
Set `MAGNETAR_PULSAR_IMAGE_TAG` to an explicit tag for compatibility runs; the issue #331 unchanged-code reproduction used `4.0.4`.
The scalable-topic suite pins `apachepulsar/pulsar:5.0.0-M1`, the first published broker carrying the vendored PIP-460 wire, and also starts `apachepulsar/pulsar:4.0.4` for capability-refusal compatibility.
Its `e2e_hardened_scalable_stream_consumer_contract` is a regular test under the `scalable-topics` product feature with no `#[ignore]` or separate e2e feature; compile/discovery without Docker is not runtime evidence, while CI's all-feature run executes it whenever its runner provides Docker.
Its flow-control regression creates twelve direct Failover child-topic consumers with receiver queues of 2,000, sends five-chunk messages to one partition and small messages to the other eleven, and requires every partition to drain without reaching zero broker permits below the logical-message refill threshold.
This topology is the end-to-end verification contract for [ADR-0076](../specs/adr/0076-conserve-flow-permits-across-chunk-reassembly.md).

### e2e container memory budget

Every `pulsar standalone` container the suite starts is capped with `PULSAR_MEM = -Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g`, declared as the `PULSAR_MEM_LIMIT` const next to `image_repo()` / `image_tag()` in each `e2e_*.rs` file that starts one.

The image default is `-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g` ([`conf/pulsar_env.sh`](https://github.com/apache/pulsar/blob/master/conf/pulsar_env.sh)), which measures **~2.3 GiB RSS per idle standalone container** against **~0.95 GiB** at the capped budget.
That difference is load-bearing on CI, not cosmetic: libtest runs up to `nproc` tests of a binary in parallel — 4 on a GitHub `ubuntu-latest` runner — and the PIP-33 two-cluster compose fixture (zookeeper + 2 bookkeepers + 2 brokers, itself capped at 256m–1g) stays up for the whole job.
At stock sizes four concurrent standalones alone reserve ~9 GiB of the runner's 16 GiB, and the resulting memory pressure stalls brokers long enough that client operations blow their 30s `operation_timeout` — surfacing as `open_producer: timed out: producer target resolution exceeded operation_timeout` (or an outright OOM-kill, as [`e2e_cluster_failover.rs`](../crates/magnetar/tests/e2e_cluster_failover.rs) documents) in whichever e2e test happens to be running.

The cap is a broker-side resource budget, never a client-side timeout adjustment: no test loosens an assertion or widens a deadline to accommodate a slow broker.
Raise `PULSAR_MEM_LIMIT` in a specific file if that suite genuinely needs more heap (compaction and deep-partition topologies are the likely candidates), and say why in the const's doc comment.

That rule is about brokers **stalled** by memory pressure — it does not cover a test that deliberately drives a full container restart and then has to wait out the JVM boot it just triggered.
There the client default is measuring the wrong thing: `send_timeout` defaults to 30 s for Java parity ([ADR-0072](../specs/adr/0072-java-parity-default-send-timeout.md)), a statement about production publish latency, while the send-timeout budget of a publish relocated across a reconnect runs from the op's original `enqueued_at` and so must also cover `docker restart` → JVM boot → namespace load → `rebuild_producers` → `CommandSendReceipt`.
`BROKER_RESTART_SEND_TIMEOUT` in [`e2e_reconnect.rs`](../crates/magnetar/tests/e2e_reconnect.rs) sizes that window from a measurement recorded in its doc comment.
The distinction that keeps this honest: the assertion is unchanged (every `SendFut` must still resolve `Ok`), the deadline stays finite so the test still proves replay completes in bounded time, and the number is measured rather than inflated until green.

`cargo run -p xtask -- check-e2e-container-memory` enforces this, in the local validation chain and as a per-PR CI job.
It walks every `GenericImage::new(…)` chain under `crates/magnetar/tests/`, resolves the image repository the first constructor argument denotes — a string literal, a `&str` const, or a zero-argument accessor returning one — and requires every `apachepulsar/…` chain to carry `.with_env_var("PULSAR_MEM", …)` before `.start()`.
The gate checks that the call is present, not what budget it sets; this section governs the value.
Containers running something other than the Pulsar JVM are out of scope by resolution rather than by exemption list: [`e2e_sasl_kerberos.rs`](../crates/magnetar/tests/e2e_sasl_kerberos.rs) starts a Kerberos KDC and [`e2e_athenz_zts.rs`](../crates/magnetar/tests/e2e_athenz_zts.rs) an Athenz ZTS server.
A chain whose image cannot be resolved, or that stashes the builder instead of reaching `.start()` in the same chain, fails the gate rather than passing unverified — an unverifiable chain is the hole this check exists to close.

Suites cover:

| File                                                                                            | Coverage                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`e2e_pulsar.rs`](../crates/magnetar/tests/e2e_pulsar.rs)                                       | Basic producer + consumer round-trip.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| [`e2e_schemas.rs`](../crates/magnetar/tests/e2e_schemas.rs)                                     | Bytes / String / JSON / Int32 schemas.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`e2e_schemas_extended.rs`](../crates/magnetar/tests/e2e_schemas_extended.rs)                   | Avro, Protobuf, KeyValue, ProtobufNative.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [`e2e_dlq.rs`](../crates/magnetar/tests/e2e_dlq.rs)                                             | DLQ + `reconsume_later`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| [`e2e_batch_chunk.rs`](../crates/magnetar/tests/e2e_batch_chunk.rs)                             | Batching + PIP-37 chunking, including the twelve-partition queue-2,000 Failover starvation regression for per-chunk flow replenishment.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [`e2e_interceptors_ack.rs`](../crates/magnetar/tests/e2e_interceptors_ack.rs)                   | Interceptor SPIs + ack patterns.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| [`e2e_transactions.rs`](../crates/magnetar/tests/e2e_transactions.rs)                           | PIP-31 commit / abort.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`e2e_sub_types.rs`](../crates/magnetar/tests/e2e_sub_types.rs)                                 | Shared / Failover / Key_Shared.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| [`e2e_partitioned_deep.rs`](../crates/magnetar/tests/e2e_partitioned_deep.rs)                   | Partitioned producer + consumer.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| [`e2e_compacted.rs`](../crates/magnetar/tests/e2e_compacted.rs)                                 | Compacted topics + TableView (PIP-94).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`e2e_persistence.rs`](../crates/magnetar/tests/e2e_persistence.rs)                             | Persistent + non-persistent semantics.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`e2e_crypto.rs`](../crates/magnetar/tests/e2e_crypto.rs)                                       | PIP-4 + `cryptoFailureAction` (Fail / Discard / Consume).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [`e2e_oauth2.rs`](../crates/magnetar/tests/e2e_oauth2.rs)                                       | OAuth2 `ClientCredentialsFlow` + token cache + refresh-on-expiry.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| [`e2e_sasl_kerberos.rs`](../crates/magnetar/tests/e2e_sasl_kerberos.rs)                         | SASL Kerberos / GSSAPI via `libgssapi` against a Dockerised MIT KDC (`gcavalcante8808/krb5-server`). Gated on `--features auth-sasl-kerberos`; needs `libkrb5-dev` + `libclang-dev` on the build host. See [ADR-0029](../specs/adr/0029-sasl-kerberos-gssapi-scope.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [`e2e_dns_resolver.rs`](../crates/magnetar/tests/e2e_dns_resolver.rs)                           | Custom `DnsResolver` plumbed end-to-end.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| [`e2e_force_unsubscribe.rs`](../crates/magnetar/tests/e2e_force_unsubscribe.rs)                 | PIP-313 force unsubscribe.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| [`e2e_memory_limit.rs`](../crates/magnetar/tests/e2e_memory_limit.rs)                           | `MemoryLimitPolicy::{FailImmediately, ProducerBlock}`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`e2e_reconnect_safety.rs`](../crates/magnetar/tests/e2e_reconnect_safety.rs)                   | Frame-aware cuts before/after producer-batch flush, conservative PIP-54 `ack_set` reconstruction with the broker feature enabled, broker-authoritative durable reattach cursors, and non-durable reattach from the original start position when a higher ack remains unconfirmed (issues #395, #396, #398, #403; ADR-0096, ADR-0099).                                                                                                                                                                                                                                                                                                                                                                    |
| [`e2e_pattern_auto_reconcile.rs`](../crates/magnetar/tests/e2e_pattern_auto_reconcile.rs)       | PIP-145 background-ticker rediscovery.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`e2e_reconnect.rs`](../crates/magnetar/tests/e2e_reconnect.rs)                                 | Supervised reconnect under broker stop/start.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| [`e2e_rolling_stats.rs`](../crates/magnetar/tests/e2e_rolling_stats.rs)                         | Rolling-window stats (msgs/sec, bytes/sec, latency p50/p99/max).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| [`e2e_seek_per_partition.rs`](../crates/magnetar/tests/e2e_seek_per_partition.rs)               | Per-partition seek callbacks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| [`e2e_cluster_failover.rs`](../crates/magnetar/tests/e2e_cluster_failover.rs)                   | PIP-121 manual cluster swap with two broker containers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [`e2e_shadow_topic.rs`](../crates/magnetar/tests/e2e_shadow_topic.rs)                           | PIP-180 — admin REST shadow-topic management, `send_with_source_message_id` propagation, `MessageReceivedFromShadow` consumer event.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| [`e2e_replicated_subscriptions.rs`](../crates/magnetar/tests/e2e_replicated_subscriptions.rs)   | PIP-33 cursor-resume across two clusters. Runs on every PR per [ADR-0046](../specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md). The `test` job in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) brings up the two-cluster docker-compose fixture (`fixtures/docker-compose.replicated-subs.yml`) before `cargo test`.                                                                                                                                                                                                                                                                                                                                                                                                                |
| [`e2e_scalable_topic.rs`](../crates/magnetar/tests/e2e_scalable_topic.rs)                       | Pulsar 5.0.0-M1 scalable lookup and namespace-watch compatibility plus the ADR-0102 public assignment-driven aggregate contract: typed multi-segment delivery, vector ack, broker-effective inclusive vector-seek replay, transaction commit/abort, single-member split progression in Strict mode, BrokerManaged cross-member behavior, direct-bootstrap controller fallback when M1 omits its controller URL, reachable broker-authored segment authorities matching the standalone bootstrap transport, and logical-close membership residue. Because the broker controls child assignment and uses one controller/bootstrap endpoint, this is not evidence that client-side Strict gating caused the pause or that same-cluster multi-broker routing works. |
| [`e2e_driver_mid_session_reject.rs`](../crates/magnetar/tests/e2e_driver_mid_session_reject.rs) | Driver recovery plus ADR-0080 operation retry: a frame-aware gate rejects the first provisional producer-open with `ProducerBusy`, then verifies that the configured retry reaches a real broker and creates the producer.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |

### Running the PIP-33 two-cluster fixture locally

`e2e_replicated_subscriptions.rs` is the one e2e test whose broker topology `testcontainers` does not spawn.
CI brings the fixture up for you; locally it takes **two** steps, not one:

```bash
cd crates/magnetar/tests/fixtures
docker compose -f docker-compose.replicated-subs.yml up -d
./configure_replicated_subs.sh
```

`up -d` bootstraps each cluster's own metadata (the `pulsar-init` service) and leaves both brokers healthy, but it cannot register the two clusters as each other's peers — that needs the admin REST endpoints, which only answer once the brokers are up.
Skip `configure_replicated_subs.sh` and the replicated-subscription tests have nothing to replicate between.
Tear down with `docker compose -f docker-compose.replicated-subs.yml down -v`.

## The `#[ignore]` policy

Per [ADR-0021](../specs/adr/0021-no-silent-test-ignore-or-remove.md): `#[ignore]` is reserved for environment dependencies the build host cannot satisfy.
Every `#[ignore]` annotation must:

1. Carry a reason string (`#[ignore = "e2e: requires Docker"]`, `#[ignore = "m8-followup: …"]`).
2. Either gate on an actual environment requirement (Docker, network), **or** link to a tracked follow-up in [`follow-ups.md`](follow-ups.md).

Bug-hiders are not acceptable.
If a test fails, fix the underlying defect or remove the test with a written rationale; do not paper over it with a silent `#[ignore]`.

## Mutation testing (scoped)

```bash
cargo mutants --package magnetar-proto --timeout 60 --shard 1/4
```

Targets frame decode, request correlation, resend/dedup, flow permits, chunk metadata, timeout transitions.
Time-boxed and run nightly + `workflow_dispatch`.

## Fuzz

```bash
cargo +nightly fuzz run encode_roundtrip
```

Round-trip-encodes `BaseCommand` shapes and asserts re-decode equality.
Lives in [`crates/magnetar-proto/fuzz/`](../crates/magnetar-proto/fuzz/).
Requires nightly; orthogonal to the moonpool engine.
