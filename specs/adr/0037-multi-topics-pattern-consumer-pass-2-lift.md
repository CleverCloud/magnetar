# ADR-0037 — MultiTopicsConsumer / PatternConsumer pass-2 lift: extend `ConsumerApi`, introduce `BrokerMetadataApi`

- **Status**: Accepted
- **Date**: 2026-05-26
- **Decider**: Florentin Dubois
- **Tags**: engine, facade, consumer-api, broker-metadata, surface-lift, ADR-0026-D1

## Context

[ADR-0026](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
§D1 set the rule: every dependent façade surface ships as a concrete
generic `magnetar::<Surface><T, E: Engine>` (no GATs), with engine
selection driven by per-surface extension traits implemented on each
runtime's `Client` / `Producer` / `Consumer` type. By 2026-05-25 the
following surfaces were lifted on both engines: Transaction (`PIP-31`),
Reader, TableView, PartitionedProducer. **MultiTopicsConsumer**,
**PartitionedConsumer**, and **PatternConsumer (PIP-145)** carried
their cascading type parameter (`MultiTopicsConsumer<C>`,
`PatternConsumer<C>`) — but the inherent impl methods stayed bound to
the tokio default `C`, because:

1. The 13 multi-topic / DLQ helper methods
   (`available_in_queue`, `available_permits`,
   `has_received_any_message`, `has_reached_end_of_topic`, `is_paused`,
   `is_inactive`, `drain_dead_letter`, `receive_with_timeout`,
   `receive_batch`, `receive_batch_with_bytes_cap`,
   `unsubscribe(force: bool)`, `reconsume_later`,
   `reconsume_later_with_properties`, `republish_dead_letters`) lived
   on each runtime's concrete `Consumer` but were absent from the
   `ConsumerApi` trait. The four flow-control + seek primitives
   (`pause`, `resume`, `seek_to_message`, `seek_to_timestamp`) were
   in the same boat.
2. `MultiTopicsConsumerBuilder<'a>` and `PatternConsumerBuilder<'a>`
   carried no engine parameter, so their `.subscribe()` dispatched
   through the tokio-bound concrete `ConsumerBuilder` rather than the
   engine-generic `ConsumerBuilder<'a, E>` that
   [SubscribeApi](../../crates/magnetar/src/engine.rs#L548-L569) already
   shipped.
3. `PulsarClient::partitions_for_topic` and
   `PulsarClient::topic_list_snapshot` lived on
   `impl PulsarClient<TokioEngine>` because they called the
   tokio-specific `runtime_client().partitioned_topic_metadata(...)` /
   `.watch_topic_list(...)` directly — no engine-generic dispatch
   trait existed.
4. `republish_dead_letters` / `reconsume_later` /
   `reconsume_later_with_properties` accept a `&Producer` argument.
   Plumbing them through a trait would either require a tokio-only
   carve-out or a way to bind the runtime's matched producer type to
   the consumer's trait surface.

Pass-1 (commits `5f1368f`, `53669f9`, `0f95a3c`, `008abbf`) ported
those 13 helpers onto the moonpool runtime so both runtimes' concrete
`Consumer` types had matching surfaces. Pass-2 is the trait + façade
lift that exposes those helpers through `ConsumerApi` and lets
`MultiTopicsConsumer<C>` / `PatternConsumer<C>` dispatch through the
trait on every method.

## Decision

Extend `magnetar::engine` with the trait surface needed to make
**every** `MultiTopicsConsumer<C>` and `PatternConsumer<C>` method
engine-generic, then route all three façade builders
(`MultiTopicsConsumerBuilder<'a, E>`,
`PartitionedConsumerBuilder<'a, E>`,
`PatternConsumerBuilder<'a, E>`) through the engine-generic base
`ConsumerBuilder<'a, E>`.

Concretely:

- **`ConsumerApi` extensions** (17 trait additions on top of the
  pass-1 base):
  - 13 pass-1 helpers (`available_in_queue`, `available_permits`,
    `has_received_any_message`, `has_reached_end_of_topic`,
    `is_paused`, `is_inactive`, `drain_dead_letter`,
    `receive_with_timeout`, `receive_batch`,
    `receive_batch_with_bytes_cap`,
    `unsubscribe(force: bool)` (overloads the prior zero-arg
    `unsubscribe`), `reconsume_later`,
    `reconsume_later_with_properties`, `republish_dead_letters`).
  - 4 pre-existing primitives (`pause`, `resume`, `seek_to_message`,
    `seek_to_timestamp`) needed by the multi-topics / pattern surfaces
    but never lifted in pass-1.
  - **`type Producer: ProducerApi<Error = Self::Error>` associated
    type** that ties each engine's `Consumer` to its matching
    `Producer`. `republish_dead_letters` /
    `reconsume_later` / `reconsume_later_with_properties` take
    `&Self::Producer` at the trait level — no tokio-only carve-out
    needed.

- **`BrokerMetadataApi` extension trait** implemented on both
  runtime `Client` types, surfacing `partitioned_topic_metadata`,
  `watch_topic_list`, and `poll_topic_list_change`. The
  PIP-145 delta type is collapsed at the trait boundary into a
  single façade-side
  [`magnetar::TopicListChange`](../../crates/magnetar/src/engine.rs)
  struct — both runtimes' identically-shaped `TopicListChange`
  structs are converted in their `impl BrokerMetadataApi` so generic
  surfaces (`PatternConsumer<C>::update`) see one shared type
  regardless of the engine.

- **`PulsarClient::partitions_for_topic` /
  `PulsarClient::topic_list_snapshot`** move from
  `impl PulsarClient<TokioEngine>` to
  `impl<E: Engine> PulsarClient<E> where E::ClientState:
  BrokerMetadataApi`.

- **`MultiTopicsConsumer<C>` / `PatternConsumer<C>` impl
  bodies** dispatch every method through the trait. Methods that
  take `&Producer` (`reconsume_later*`, `republish_dead_letters` —
  the latter only at the underlying Consumer level; the
  MultiTopics-scope variant uses the same `&C::Producer` parameter)
  use the associated `Producer` type.
  No tokio-only carve-out is needed.

- **`MultiTopicsConsumerBuilder<'a, E: Engine = TokioEngine>`,
  `PartitionedConsumerBuilder<'a, E: Engine = TokioEngine>`,
  `PatternConsumerBuilder<'a, E: Engine = TokioEngine>`** all carry
  the engine parameter. `.subscribe()` is gated on
  `where E::ClientState: SubscribeApi + BrokerMetadataApi,
  <E::ClientState as SubscribeApi>::Consumer: Clone` (the
  Consumer-Clone bound matches the existing assumption inside the
  MultiTopicsConsumer / PatternConsumer surface — both runtimes'
  `Consumer` types are `Clone`).

- **PIP-145 child-subscribe in
  `PatternConsumer<C>::update` / `start_auto_reconcile`** routes
  through `<E::ClientState as SubscribeApi>::subscribe` (via
  `client.consumer(topic).subscribe()`); the delta drain routes
  through `<E::ClientState as BrokerMetadataApi>::poll_topic_list_change`.

- **`Inner<C>::auto_update`** keeps holding a tokio
  `JoinHandle<()>` + `tokio::sync::Notify`. Both engines'
  `Engine::TaskHandle` resolve to `tokio::task::JoinHandle<()>` (see
  `MoonpoolEngine`'s `type TaskHandle = tokio::task::JoinHandle<()>`)
  so this is engine-invariant, not a tokio carve-out.

## Consequences

**Easier:**

- Code targeting the moonpool engine can now use the full
  `MultiTopicsConsumer` / `PartitionedConsumer` / `PatternConsumer`
  surface. The differential harness can write coordinated
  multi-topic workloads on both engines without lifting the type
  signature outside the workspace.
- The `ConsumerApi` trait is now broad enough that any future
  multi-consumer aggregator can build on top of it without lifting
  yet more methods (the trait is "comprehensive" — see
  `docs/parity-status.md`).
- `PulsarClient::partitions_for_topic` and
  `PulsarClient::topic_list_snapshot` are now engine-generic, so
  any caller that holds `&PulsarClient<E>` rather than the tokio
  default gains those methods automatically (gated on the
  `BrokerMetadataApi` bound).
- The matched `type Producer: ProducerApi` associated type sets a
  precedent for cleanly pairing producer + consumer roles in future
  trait extensions (e.g. a future "TransactionalProducer" surface).

**Harder / costlier:**

- The `ConsumerApi` trait now has 41 methods and one associated
  type. Adding a new runtime is more work than before. This is the
  current target surface; the trait is additive so subsequent
  extensions are non-breaking, but the up-front cost of a new
  runtime is now real.
- The trait method `unsubscribe(force: bool)` changed signature
  from the pass-1 `unsubscribe()` (no force). No external callers
  were exercising the trait `unsubscribe()`; every caller went
  through the concrete runtime `Consumer::unsubscribe(force)`. The
  break is contained.
- The trait method `receive` (and the `receive_with_timeout` /
  `receive_batch` family) now returns
  `magnetar_proto::IncomingMessage` directly (no wrapping to
  `crate::IncomingMessage`). This eliminates a redundant conversion
  layer but means downstream callers that want the façade-side
  type need to call `.into()` themselves. The wrap was a
  no-information-added abstraction; the trait is more honest now.

**Incompatible with:**

- Engines whose `Consumer` is not `Clone` (the multi-topics
  coordinator clones consumers into snapshots before awaiting).
  Both runtimes today implement `Clone` cheaply (every clone
  shares the underlying `Arc<ConnectionShared>`), so this isn't a
  live constraint — but a future engine that needed unique
  ownership of its `Consumer` would have to either provide a
  cheap shared-ownership wrapper or stay outside the multi-topics
  surface.

## References

- [`crates/magnetar/src/engine.rs`](../../crates/magnetar/src/engine.rs) —
  `ConsumerApi` extensions + `BrokerMetadataApi` + `TopicListChange`.
- [`crates/magnetar/src/multi_topics.rs`](../../crates/magnetar/src/multi_topics.rs) —
  `MultiTopicsConsumer<C>` impl-body lift.
- [`crates/magnetar/src/pattern_consumer.rs`](../../crates/magnetar/src/pattern_consumer.rs) —
  `PatternConsumer<C>` impl-body lift + PIP-145 auto-reconcile via
  `SubscribeApi`.
- [`crates/magnetar/src/partitioned_consumer.rs`](../../crates/magnetar/src/partitioned_consumer.rs) —
  `PartitionedConsumerBuilder<'a, E>` lift.
- [`docs/parity-status.md`](../../docs/parity-status.md) — engine
  parity row flips.
- [`docs/follow-ups.md`](../../docs/follow-ups.md) — six-of-seven
  D1 surfaces now lifted.
- [ADR-0019](0019-engine-scope-and-moonpool-parity.md) — `PulsarClient<E>`
  generic + no silent tokio fallback.
- [ADR-0026](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
  §D1 — concrete-generic surfaces over GATs.
