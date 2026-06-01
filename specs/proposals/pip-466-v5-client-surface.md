# PIP-466 — V5 client surface (experimental)

- **Status**: Draft
- **ADR**: [ADR-0032](../adr/0032-pip-466-v5-client-surface-scope.md)
- **Date**: 2026-05-26
- **Owner**: Florentin Dubois
- **Upstream**: [pip/pip-466.md](https://github.com/apache/pulsar/blob/master/pip/pip-466.md)
- **Upstream readiness**: 🟠 **DESIGN-PHASE.** PIP-466 is the Java V5 client redesign and is **still iterating upstream**; no stable Pulsar release exposes the V5 Java modules as default.
  Magnetar's V5 surface is a thin skin over the v4 wire — which **is** live — so the surface works against current Pulsar 4.x brokers today.
  The `🟡 experimental` tag captures upstream-design churn risk, not "blocked on broker."
- **Broker baseline**: Pulsar 4.0+ for the non-scalable subset of the V5 surface; the V5 `CheckpointConsumer` is out of scope and would require Pulsar 5.0+ via PIP-460.

## TL;DR

PIP-466 is a clean-slate redesign of the Pulsar **Java** client API, designed to live **alongside** the v4 API as a separate module set (no wire change beyond the one `CommandScalableTopicLookup` already covered by [PIP-460](pip-460-scalable-topics.md)). magnetar ships a **subset** that mirrors the V5 ergonomic shape — `Duration` types, `Option<T>` builder fields, `StreamConsumer` / `QueueConsumer` roles — as a **thin skin over the v4 surface** behind a default-off feature flag.
We do not duplicate the v4 surface end-to-end and we do not introduce per-surface engine GATs.

## 1. Wire-protocol delta vs. vendored `PulsarApi.proto`

**None.**

PIP-466 is purely a client-side API redesign.
The only new wire bit it introduces — `CommandScalableTopicLookup` — is shared with PIP-460 and is vendored under [PIP-460 §1](pip-460-scalable-topics.md#1-wire-protocol-delta-vs-vendored-pulsarapiproto).
This proposal does **not** drive a second proto bump.

The V5 surface that we ship runs **bit-for-bit on the v4 wire**:

| V5 surface                                     | v4 wire delegate                                                 |
| ---------------------------------------------- | ---------------------------------------------------------------- |
| `v5::StreamConsumer<T>` (Exclusive / Failover) | v4 `Consumer<T>` with `SubscriptionType::{Exclusive, Failover}`. |
| `v5::QueueConsumer<T>` (Shared / KeyShared)    | v4 `Consumer<T>` with `SubscriptionType::{Shared, KeyShared}`.   |
| `v5::Producer<T>`                              | v4 `Producer<T>`.                                                |

## 2. `magnetar-proto` state-machine additions

**None.**

PIP-466 reuses the existing v4 wire driver (`Conn`, `Producer` / `Consumer` trackers, `Reader`).
The V5 surface is a thin re-skin over the same sans-io machinery.
No new `Event` variant, no new `Conn` entry, no new tracker type.

## 3. Runtime surface ports

### 3.1 `magnetar-runtime-tokio` (and the `magnetar` façade)

V5 surfaces live on the `magnetar` façade (the public crate), generic over `E: Engine`, so the tokio engine and the moonpool engine share one set of types per [ADR-0026 §D1](../adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md).

| File                                                             | Change                                                                                            |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `crates/magnetar/src/v5/mod.rs` (NEW)                            | Module root; `pub mod client; pub mod producer; pub mod stream_consumer; pub mod queue_consumer;` |
| `crates/magnetar/src/v5/client.rs` (NEW)                         | `PulsarClientV5<E>` wraps `Arc<E::ClientState>`.                                                  |
| `crates/magnetar/src/v5/producer.rs` (NEW)                       | `Producer<T, E>` skin; `Duration`-typed `send_timeout`.                                           |
| `crates/magnetar/src/v5/stream_consumer.rs` (NEW)                | Ordered consume; maps to `SubscriptionType::Exclusive` / `Failover`.                              |
| `crates/magnetar/src/v5/queue_consumer.rs` (NEW)                 | Parallel-work consume; maps to `Shared` / `KeyShared`.                                            |
| [`crates/magnetar/src/lib.rs`](../../crates/magnetar/src/lib.rs) | `#[cfg(feature = "experimental-v5-client")] pub mod v5;` re-export.                               |

#### Surface map

```rust
// crates/magnetar/src/v5/client.rs (NEW)
pub struct PulsarClientV5<E: Engine> {
    state: Arc<E::ClientState>,
}

impl<E: Engine> PulsarClientV5<E> {
    pub fn builder() -> PulsarClientV5Builder<E> { /* … */ }

    /// Underlying v4 client — escape hatch for surfaces not yet
    /// ported to V5 (Reader, TableView, Transaction).
    pub fn v4(&self) -> PulsarClient<E> { /* … */ }
}

// crates/magnetar/src/v5/producer.rs (NEW)
pub struct Producer<T, E: Engine> {
    inner: crate::Producer<T, E>,
}

impl<T, E: Engine> Producer<T, E> {
    pub fn builder() -> ProducerBuilder<T, E> { /* … */ }
    pub async fn send(&self, msg: T) -> Result<Option<MessageId>, Error>;
    pub fn send_timeout(&self) -> Duration;
    /* … */
}

pub struct ProducerBuilder<T, E: Engine> { /* Option-typed fields */ }

impl<T, E: Engine> ProducerBuilder<T, E> {
    pub fn topic(self, topic: impl Into<TopicName>) -> Self;
    pub fn send_timeout(self, d: Duration) -> Self;          // Duration, not u64
    pub fn max_pending_messages(self, n: Option<usize>) -> Self;  // Option, not "0 = unlimited"
    pub fn build(self) -> Result<Producer<T, E>, Error>;
}
```

`StreamConsumer` and `QueueConsumer` follow the same shape — a thin struct holding the v4 `Consumer<T, E>` and a builder using `Duration`

- `Option<T>` semantics.

#### V5 → v4 mapping table

The single source of truth for the V5 builder defaults and translation.
Lives in `crates/magnetar/src/v5/mapping.rs` (NEW) as a documented set of `const` mappings so it is reviewable in one file:

| V5 field                                  | v4 field                                                 | Default                   | Notes                 |
| ----------------------------------------- | -------------------------------------------------------- | ------------------------- | --------------------- |
| `send_timeout: Duration`                  | `producer.send_timeout_ms: u64`                          | `Duration::from_secs(30)` | `as_millis() as u64`. |
| `max_pending_messages: Option<usize>`     | `producer.max_pending_messages: usize` (`0 = unlimited`) | `Some(1000)`              | `None → 0`.           |
| `subscription_initial_position: Position` | `consumer.initial_position`                              | `Latest`                  | Direct.               |
| `negative_ack_redelivery_delay: Duration` | `consumer.negative_ack_redelivery_delay_ms`              | `Duration::from_secs(60)` | `as_millis() as u64`. |
| `ack_timeout: Option<Duration>`           | `consumer.ack_timeout_ms` (`0 = disabled`)               | `None`                    | `None → 0`.           |
| `receiver_queue_size: usize`              | same                                                     | `1000`                    | Direct.               |

#### Feature flag

`experimental-v5-client` on the `magnetar` crate, **default off**.

```toml
# crates/magnetar/Cargo.toml
[features]
experimental-v5-client = []
```

The `magnetar-runtime-tokio` crate does not need a parallel feature because V5 types live on the façade and are `<E: Engine>`-generic.

#### Doc banner

Every public V5 type carries:

```rust
#[doc = "**Experimental** — PIP-466 V5 client surface."]
#[doc = "Behaviour and signatures may change before V5 is promoted to default."]
```

### 3.2 `magnetar-runtime-moonpool`

**Inherits the V5 surface for free.** Because V5 types are `<E: Engine>`-generic and delegate to v4 surfaces that already work under `MoonpoolEngine<P>` ([ADR-0027](../adr/0027-moonpool-engine-clientstate-is-runtime-client.md)), the moonpool engine gets V5 the moment it is exported from the façade.
No new sim-side broker fake.
No new `BrokerWorkload` variant.
No new scripted scenario.

### 3.3 `magnetar-cli`

No V5-specific subcommand yet.
The CLI continues to use the v4 API internally; advertising the V5 module from the CLI is deferred to a follow-up.

## 4. Four-layer test plan ([ADR-0024](../adr/0024-cross-runtime-test-and-coverage-policy.md))

### Exemptions claimed and justified

| Layer                                       | Status                                                                                  | Justification (per ADR-0024 §Exemptions)                                                                                                       |
| ------------------------------------------- | --------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| (a) `magnetar-proto` unit                   | **Exempt**                                                                              | No wire or sans-io surface change. Commit message must reference this proposal.                                                                |
| (b) `magnetar-runtime-tokio` integration    | **Required** — V5 ↔ v4 mapping is the binding behaviour.                                |
| (c) `magnetar-runtime-moonpool` integration | **Required** — must be 1:1 with (b) per ADR-0024, even though no sim-side fake changes. |
| (d) `magnetar-differential` equivalence     | **Exempt**                                                                              | No new sans-io surface; V5 is a thin re-skin over v4, whose differential coverage already exists. Commit message must reference this proposal. |

### (b) `magnetar-runtime-tokio` integration

`crates/magnetar/tests/v5_*.rs` (NEW), gated on `feature = "experimental-v5-client"`.
One file per surface:

| Test file                       | Tests                                                                                                                                            |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `v5_producer_mapping.rs`        | `producer_send_timeout_translates_to_v4_ms`; `producer_max_pending_none_maps_to_unlimited`; `producer_send_returns_optional_message_id`.         |
| `v5_stream_consumer_mapping.rs` | `stream_consumer_exclusive_subscription_type`; `stream_consumer_failover_subscription_type`; `stream_consumer_negative_ack_duration_translates`. |
| `v5_queue_consumer_mapping.rs`  | `queue_consumer_shared_subscription_type`; `queue_consumer_key_shared_subscription_type`; `queue_consumer_receiver_queue_size_translates`.       |
| `v5_client_v4_escape_hatch.rs`  | `v5_client_v4_returns_underlying_v4_client_with_shared_state` (no double-init of `ClientState`).                                                 |
| `v5_builder_defaults.rs`        | One table-driven test per row of the V5→v4 mapping table in §3.1.                                                                                |

Each test wires a `magnetar-fakes` scripted broker, builds the v5 surface, sends/receives, asserts the **wire bytes** the broker observes match the v4 expectation.
The translation layer is what's under test, not the v4 wire — but the v4 wire is the ground truth.

### (c) `magnetar-runtime-moonpool` integration

**Same five test files, same test names, 1:1 with (b).** Located at `crates/magnetar/tests/v5_*_moonpool.rs`, or — once we settle on a parameterised pattern — share `tests/v5_*.rs` and select the engine via a `cfg` shim.
Decision: **separate files**, mirrored, to preserve clarity of the test-parity invariant `cargo xtask check-runtime-test-parity` enforces.

Coverage: `cargo xtask check-sim-coverage` enforces 100% diff coverage on the new `magnetar::v5` module.
Because the module is a thin skin, the tests above are sufficient — every line in `mapping.rs` is touched by the table-driven test in `v5_builder_defaults.rs`.

### (d) `magnetar-differential`

Exempt as noted.
The v4 surface's existing differential coverage in [`crates/magnetar-differential/tests/`](../../crates/magnetar-differential/tests/) remains the binding contract.

### Bookkeeping

The commit landing the V5 surface **must** carry these lines in the message body so `cargo xtask check-runtime-test-parity` and reviewer attention land in the right place:

```
test-exemption-proto: PIP-466 V5 surface (no wire/sans-io change)
test-exemption-differential: PIP-466 V5 surface (no new sans-io surface)
```

## 5. E2E plan

No new e2e fixture.
The existing [`crates/magnetar/tests/e2e_*.rs`](../../crates/magnetar/tests/) suite is parameterised at the test level to run **once per client variant** on representative tests:

| e2e test                                     | v4 variant | v5 variant                                           |
| -------------------------------------------- | ---------- | ---------------------------------------------------- |
| `e2e_pulsar::send_receive_roundtrip`         | existing   | NEW — `e2e_pulsar_v5::send_receive_roundtrip`        |
| `e2e_sub_types::shared_consumer_round_robin` | existing   | NEW — `e2e_sub_types_v5::queue_consumer_round_robin` |
| `e2e_sub_types::exclusive_consumer`          | existing   | NEW — `e2e_sub_types_v5::stream_consumer_exclusive`  |

These three V5 e2e tests pin the V5 ↔ v4 mapping against a **real broker** (`apachepulsar/pulsar:4.0.4`, no new image).
Gated on `feature = "e2e,experimental-v5-client"`, retain the `#[ignore = "e2e: requires Docker"]` attribute used elsewhere.

The Pulsar 5.0 only `CommandScalableTopicLookup` path is **owned by [PIP-460](pip-460-scalable-topics.md)'s e2e**, not by this proposal.

## 6. LOC + risk

| Component                                                   | LOC est.  |
| ----------------------------------------------------------- | --------- |
| `magnetar::v5` module (3 surface types + client + builders) | ~300      |
| `magnetar::v5::mapping` constants + helpers                 | ~80       |
| v4 ↔ v5 delegation glue                                     | ~150      |
| Tests (b) + (c) mirrored                                    | ~300      |
| Doc-tests + module banner                                   | ~50       |
| README parity-matrix row update                             | ~50       |
| E2E mirror tests (×3)                                       | ~150      |
| **Total**                                                   | **~1080** |

### Risks

1. **Upstream V5 churn.** PIP-466 is itself in active design upstream — V5 method names, default values, and even module layout may shift before Pulsar 5.0 GA.
   Mitigation: the experimental banner + default-off feature flag mean breaking changes do not affect callers who stay on the v4 surface.
2. **API duplication tax.** Maintaining two surfaces means every v4 bugfix needs a "does the V5 skin still translate?" review.
   Mitigation: the table-driven `v5_builder_defaults.rs` test catches default-value drift; the per-surface mapping tests catch typed-field drift; the v4 ↔ v5 escape hatch (`v5().v4()`) keeps the surfaces from forking semantically.
3. **`Option<Duration>` vs. `0 = unlimited` translation foot-guns.** Documented in `crates/magnetar/src/v5/mapping.rs` and tested exhaustively in `v5_builder_defaults.rs`.
   The table in §3.1 is the single source of truth.
4. **CheckpointConsumer absence may surprise users.** Mitigation: parity-matrix banner explicitly lists which V5 surfaces are present, and a `// TODO: PIP-466 CheckpointConsumer (follow-up, blocked on PIP-460 controller-broker work)` marker lives at the bottom of `crates/magnetar/src/v5/mod.rs`.

### Rollback

The V5 surface is feature-flagged off by default.
Rollback is trivially `cargo build --no-default-features`.
No revert PR needed.

## 7. Dependencies + sequencing

V5 has no wire or sans-io prereq beyond what already ships on the v4 surface.
PIP-466 can land **in parallel** with PIP-460, PIP-180, and PIP-33.

1. **Wave 1**: `magnetar::v5` module skeleton (`mod.rs`, `mapping.rs`)
   - (b) `v5_builder_defaults.rs` table-driven test.
     No surface impl.
2. **Wave 2**: `Producer<T, E>` + tests.
   `StreamConsumer<T, E>` + tests.
   `QueueConsumer<T, E>` + tests.
3. **Wave 3**: `PulsarClientV5<E>` + v4-escape-hatch test.
4. **Wave 4**: Moonpool mirror tests (1:1 parity).
5. **Wave 5**: docs ([`docs/pip-features.md#v5-client-surface-pip-466`](../../docs/pip-features.md#v5-client-surface-pip-466); README).
6. **Wave 6**: e2e mirror tests (×3).

## 8. Documentation deliverables (same wave)

- [`docs/pip-features.md#v5-client-surface-pip-466`](../../docs/pip-features.md#v5-client-surface-pip-466) — V5 surface overview, experimental banner, mapping table, examples.
- [`README.md`](../../README.md#java-client-parity-matrix) — parity-matrix row update (canonical row); the PIP-466 entry lands marked `🟡 experimental — Stream/Queue consumers + Producer; no Reader/TableView/Transaction/CheckpointConsumer in V5 module`.
- `specs/README.md` — flip ADR-0032 to `Accepted` on sign-off.
- `docs/follow-ups.md` — follow-up list: V5 `Reader`, `TableView`, `Transaction`, `CheckpointConsumer`; V5-by-default decision.

## 9. References

- [ADR-0032](../adr/0032-pip-466-v5-client-surface-scope.md) — scope.
- [ADR-0024](../adr/0024-cross-runtime-test-and-coverage-policy.md) — test plan binding (claims (a)+(d) exemptions, justified above).
- [ADR-0026 §D1](../adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) — `Surface<T, E>` design; V5 surfaces take `E: Engine` directly.
- [ADR-0027](../adr/0027-moonpool-engine-clientstate-is-runtime-client.md) — `MoonpoolEngine::ClientState = Client<P>`; moonpool inherits V5 surfaces for free.
- Upstream PIP — [pip/pip-466.md](https://github.com/apache/pulsar/blob/master/pip/pip-466.md).
- Companion proposal — [PIP-460](pip-460-scalable-topics.md) carries the only PIP-466-adjacent wire change.
