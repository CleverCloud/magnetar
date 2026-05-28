# Magnetar V5 client surface (PIP-466)

**Status**: experimental (gated `feature = "experimental-v5-client"`,
default off). The surface ships in `magnetar v0.2.0` against Pulsar 4.x
brokers. Upstream Java V5 is still iterating; magnetar's V5 surface is
a thin wrapper around the v4 wire commands, with V5-shaped types
(`Duration`, `Option<usize>`, `V5SubscriptionInitialPosition`) on the
caller-facing builders.

Locked by [ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)
(Proposed). See [`docs/parity-status.md`](parity-status.md) for the
parity-matrix row.

## When to use V5

- You want the Java V5 ergonomics today (`Duration`-typed timeouts,
  per-surface `StreamConsumer` vs `QueueConsumer` builders, named
  initial-position enum) without waiting for the upstream V5 release
  to ship.
- You're building greenfield code on Pulsar 4.x — the V5 builders
  translate to the existing v4 wire commands, so there's no broker
  version gate.
- You're prototyping the V5 migration for an existing v4 codebase and
  want to mix-and-match V5 + v4 surfaces against the same connection
  (the [`PulsarClientV5::v4()`](#v4-escape-hatch) escape hatch returns
  the inner v4 client, no double-init).

## When NOT to use V5

- You need a feature V5 hasn't lifted yet — `Reader`, `TableView`,
  transactions. Stay on the v4 surface; the V5 wrapper exposes
  `v4()` if you need a mixed setup.
- You're shipping to production with strict surface-stability
  requirements. V5 is experimental until ADR-0032 flips to Accepted
  and the parity-matrix row to ✅ default-on.

## Enable the feature

```toml
[dependencies]
magnetar = { version = "0.2", features = ["experimental-v5-client"] }
```

V5 is mutually composable with every other magnetar feature
(`tokio`, `moonpool`, `auth-oauth2`, `encryption`, `crypto-aws-lc-rs`,
…) — it's purely an additive surface.

## Quick start

```rust
use std::time::Duration;
use magnetar::v5::{PulsarClientV5, mapping::V5SubscriptionInitialPosition};
use magnetar::PulsarClient;

// 1. Build a v4 client as usual.
let v4 = PulsarClient::builder()
    .service_url("pulsar://broker:6650")
    .build()
    .await?;

// 2. Wrap it in the V5 surface.
let client = PulsarClientV5::from_v4(v4);

// 3. Producer with V5 ergonomics.
let producer = client
    .producer("persistent://public/default/topic")
    .send_timeout(Duration::from_secs(30))
    .max_pending_messages(Some(1000))
    .create()
    .await?;

// 4. Stream consumer (Exclusive default; .failover() for Failover).
let stream = client
    .stream_consumer("persistent://public/default/topic")
    .subscription("my-sub")
    .negative_ack_redelivery_delay(Duration::from_secs(60))
    .ack_timeout(Some(Duration::from_secs(30)))
    .subscribe()
    .await?;

// 5. Queue consumer (Shared default; .key_shared() for KeyShared).
let queue = client
    .queue_consumer("persistent://public/default/queue")
    .subscription("my-queue-sub")
    .key_shared()
    .subscribe()
    .await?;
```

## V5 → v4 mapping table

The V5 builders accept `Duration` / `Option<usize>` /
`Option<Duration>` / `V5SubscriptionInitialPosition`, then translate to
the v4 wire fields via the centralised
[`v5::mapping`](../crates/magnetar/src/v5/mapping.rs) module. The
defaults match Java V5 (`org.apache.pulsar.client.api.v5`).

| V5 builder field                  | V5 type                          | V5 default        | v4 wire field                                                                    | v4 type     | Translation function                                       |
|-----------------------------------|----------------------------------|-------------------|-----------------------------------------------------------------------------------|-------------|------------------------------------------------------------|
| `send_timeout`                    | `Duration`                       | `30 s`            | `send_timeout` (millis)                                                           | `u64`       | [`send_timeout_to_ms`](../crates/magnetar/src/v5/mapping.rs) |
| `max_pending_messages`            | `Option<usize>`                  | `Some(1000)`      | `max_pending_messages` (`0` = unlimited)                                          | `usize`     | [`max_pending_messages_to_v4`](../crates/magnetar/src/v5/mapping.rs) |
| `ack_timeout`                     | `Option<Duration>`               | `None`            | `ack_timeout_ms` (`0` = disabled)                                                 | `u64`       | [`ack_timeout_to_ms`](../crates/magnetar/src/v5/mapping.rs) |
| `negative_ack_redelivery_delay`   | `Duration`                       | `60 s`            | `negative_ack_redelivery_delay_ms`                                                | `u64`       | [`negative_ack_redelivery_delay_to_ms`](../crates/magnetar/src/v5/mapping.rs) |
| `receiver_queue_size`             | `usize`                          | `1000`            | `receiver_queue_size`                                                             | `usize`     | _(direct)_                                                  |
| `subscription_initial_position`   | `V5SubscriptionInitialPosition`  | `Latest`          | `pb::command_subscribe::InitialPosition`                                          | enum        | [`V5SubscriptionInitialPosition::into_pb`](../crates/magnetar/src/v5/mapping.rs) |

### Edge cases worth knowing

- **`ack_timeout = None` vs `Some(Duration::ZERO)`** — both translate
  to wire `0` (the v4 "disabled" sentinel). The V5 type
  distinguishes them, but the v4 wire collapses both. Pinned by
  `v5_builder_defaults::v5_translation_edge_cases`.
- **`max_pending_messages = None` vs `Some(0)`** — both translate to
  wire `0` (the v4 "unlimited" sentinel). Same pin.
- **`send_timeout` saturation** — pathological `Duration` values
  beyond `u64::MAX` millis clamp at `u64::MAX` rather than panic. The
  most-permissive interpretation. Pinned by the same test.

## Subscription types

| V5 builder                                | v4 `SubType` | Notes                                                                                      |
|-------------------------------------------|--------------|--------------------------------------------------------------------------------------------|
| `client.stream_consumer(topic)` (default) | `Exclusive`  | Single active consumer per partition; ordered delivery.                                    |
| `client.stream_consumer(topic).failover()`| `Failover`   | One active consumer per partition with automatic failover to backups.                      |
| `client.queue_consumer(topic)` (default)  | `Shared`     | Work-distribution across multiple active consumers per partition; no per-key ordering.     |
| `client.queue_consumer(topic).key_shared()` | `KeyShared`| Per-key ordering across a set of active consumers. Attaches default `KeySharedMeta`.       |

## v4 escape hatch

`PulsarClientV5` holds no parallel state — it wraps the underlying v4
`PulsarClient` directly. `v4()` borrows the inner client; `into_v4()`
consumes the wrapper.

```rust
let v5 = PulsarClientV5::from_v4(v4_client);

// Mix surfaces on the same engine state:
let v4_reader = v5.v4().reader(topic).start_message_id(start).create().await?;
let v5_producer = v5.producer(topic).create().await?;

// Or migrate back wholesale:
let back_to_v4: PulsarClient = v5.into_v4();
```

ADR-0032 pins this contract via the
`v5_client_v4_escape_hatch::v5_wrapper_is_zero_sized_over_v4_client`
test — `mem::size_of::<PulsarClientV5>` must equal
`mem::size_of::<PulsarClient>`. A future refactor that added parallel
state would fail that assertion.

## Test layers

The V5 mapping translations are covered by:

| Layer        | File                                                                                                                     |
|--------------|--------------------------------------------------------------------------------------------------------------------------|
| Unit         | [`crates/magnetar/src/v5/mapping.rs::tests`](../crates/magnetar/src/v5/mapping.rs)                                       |
| Producer wire | [`crates/magnetar/tests/v5_producer_mapping.rs`](../crates/magnetar/tests/v5_producer_mapping.rs)                      |
| Stream wire  | [`crates/magnetar/tests/v5_stream_consumer_mapping.rs`](../crates/magnetar/tests/v5_stream_consumer_mapping.rs)         |
| Queue wire   | [`crates/magnetar/tests/v5_queue_consumer_mapping.rs`](../crates/magnetar/tests/v5_queue_consumer_mapping.rs)           |
| Escape hatch | [`crates/magnetar/tests/v5_client_v4_escape_hatch.rs`](../crates/magnetar/tests/v5_client_v4_escape_hatch.rs)           |
| Defaults     | [`crates/magnetar/tests/v5_builder_defaults.rs`](../crates/magnetar/tests/v5_builder_defaults.rs)                       |

The wire-byte tests use
[`magnetar_fakes::FrameRecorder`](../crates/magnetar-fakes/src/lib.rs)
to drain a sans-io `Connection` and decode the resulting frames; they
assert that V5 builder calls translate to the expected v4
`CommandProducer` / `CommandSubscribe` field values on the wire.

## Roadmap

Status snapshot — the parity-matrix row flipped from 🟡 experimental
to ✅ on 2026-05-28 when ADR-0032 was Accepted alongside the unified
engine-generic refactor (`docs/follow-ups.md` §2). The
`experimental-v5-client` feature stays default-off; acceptance flips
the matrix and unlocks moonpool-engine V5 usage, not the default-on
flag.

1. **✅ Landed (2026-05-28).** The five mapping/wire test files now
   have moonpool 1:1 mirrors at
   `crates/magnetar/tests/v5_*_moonpool.rs` (engine-shape pinning +
   sans-io wire assertions against
   `MoonpoolEngine<TokioProviders>`). The V5 surface has full
   deterministic-simulation coverage symmetric with the v4 surface.
2. Three e2e tests (`crates/magnetar/tests/e2e_pulsar_v5.rs` +
   `e2e_sub_types_v5.rs`) gated
   `feature = "e2e,experimental-v5-client"` against Pulsar 4.0.4.
3. **✅ Landed (2026-05-28).** ADR-0032 promoted from Proposed →
   Accepted; matrix sweep (`check-crypto-matrix` × V5 axis) green.
4. **✅ Landed (2026-05-28).** Engine-genericity:
   `PulsarClientV5<E: Engine = TokioEngine>` is parametric.
   `MessageEncryptor` / `MessageDecryptor` types now live behind the
   per-engine [`MessageEncryptorApi`] / [`MessageDecryptorApi`]
   extension traits (tokio plugs in
   `Arc<dyn magnetar_runtime_tokio::MessageEncryptor>`; moonpool plugs
   in `NoEncryption` no-op stub). `MessageRouter` is a façade-level
   trait (pure routing math), already engine-agnostic.
5. **✅ Landed (2026-05-28).** Per-surface builder lifts —
   `PartitionedProducerBuilder<E>`, `TableViewBuilder<E>`,
   `TypedTableViewBuilder<S, E>` are now engine-generic. The
   tokio-specialised `.create_with_encryption` /
   `.create_with_decryption` impls retain the PIP-4 carve-out.

[`MessageEncryptorApi`]: ../crates/magnetar/src/engine/mod.rs
[`MessageDecryptorApi`]: ../crates/magnetar/src/engine/mod.rs

## References

- [PIP-466 proposal](../specs/proposals/pip-466-v5-client-surface.md)
- [ADR-0032 — V5 client surface scope](../specs/adr/0032-pip-466-v5-client-surface-scope.md)
- [Apache Pulsar V5 client (Java)](https://github.com/apache/pulsar-client-reactive) — upstream design source
