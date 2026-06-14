# ADR-0064 — Consumer-side MessageListener push delivery

- **Status**: Accepted
- **Date**: 2026-06-14
- **Decider**: Florentin Dubois
- **Tags**: consumer, push-delivery, parity, runtime, sans-io

## Context

magnetar consumers were **pull-only**: `Consumer::receive` / `receive_async`, plus the batch and timeout variants.
The Java client also offers **push** delivery via `ConsumerBuilder#messageListener(MessageListener)`: when a listener is registered the client drives the receive loop itself and invokes `MessageListener#received(Consumer, Message)` once per message, sequentially, off the client's listener executor.
A workspace grep for `message_listener` / `MessageListener` returned nothing — the only listener surface was `TableView::listen` (`crates/magnetar/src/table_view.rs:240`).
This was a genuine consumer-side Java-parity gap.

The architectural constraint is that this is a **runtime** concern: `magnetar-proto` is sans-io (ADR-0004) and cannot spawn tasks or invoke callbacks.
The established way to drive a receive loop in the background already exists — `TableView`'s `spawn_drain` (`crates/magnetar/src/table_view.rs:584`) `tokio::spawn`s a `loop { receive(); … }` over a `C: ConsumerApi + Clone`, engine-generic, with a `Drop`-abort wrapper around the `JoinHandle`.
ADR-0025 blesses `tokio::spawn` for both engines (determinism for the moonpool engine comes from substituting the `moonpool_core::Providers`, not from replacing the executor).
The alternatives — a per-engine spawn seam, or a channel between the consumer and the listener — were rejected: the first duplicates the `spawn_drain` precedent for no benefit, and the second is banned by ADR-0003 (no channels).

## Decision

Add push delivery as a **runtime-side spawned poller over the existing `receive()` loop**, mirroring `TableView::listen` exactly. `magnetar-proto` is untouched.

- **Type.** `MessageListener = Arc<dyn Fn(&IncomingMessage) + Send + Sync>` (`crates/magnetar/src/consumer_listener.rs`) — a **synchronous** callback over the façade `IncomingMessage`, the same shape as `TableViewListener`. The schema-aware variant is `TypedMessageListener<S> = Arc<dyn Fn(&TypedMessage<S>) + Send + Sync>` (`crates/magnetar/src/typed.rs`).
- **Poller.** `spawn_listener_loop<C: ConsumerApi + Clone>(consumer, on_message)` `tokio::spawn`s `loop { let Ok(msg) = receive().await else { break }; on_message(msg); }`. No channel (ADR-0003), no new lock (ADR-0038 order preserved — the loop takes only what `receive()` already takes), no host-clock read (ADR-0011). It is engine-generic, so the tokio and moonpool consumers share one poller.
- **Builder surface.** `ConsumerBuilder::message_listener(MessageListener)` + `subscribe_with_listener()` and `TypedConsumerBuilder::message_listener(TypedMessageListener<S>)` + `subscribe_with_listener()`. The terminal subscribes, then spawns the poller, returning a `MessageListenerHandle` (a `Drop`-abort `JoinHandle` wrapper with `is_running()` / async `close()`, mirroring `TableView`'s `DrainTask`). The typed terminal resolves a broker-side schema once up front (if `needs_broker_schema()`) so per-message decode in the closure stays synchronous.
- **Delivery semantics — match Java.** Sequential and in order: the poller awaits one `receive()`, runs the callback to completion, then pulls the next — no per-message concurrency. **No auto-ack**: the callback acks explicitly (positive, cumulative, or nack), exactly like Java hands you the `Consumer` to ack; the poller never acks on the callback's behalf. **Clean shutdown**: `receive()` resolving with an error (closed / terminally-disconnected consumer) breaks the loop with no panic; dropping the `MessageListenerHandle` (or calling `close()`) aborts the task eagerly.
- **Pull / push mutual exclusion.** Java forbids `receive()` on a listener-backed consumer. magnetar enforces the intent structurally: `subscribe_with_listener()` **moves** the consumer into the poller task and returns only the `MessageListenerHandle`, so there is no consumer handle left to call `receive()` on. The plain `subscribe()` ignores any configured listener and returns a normal pull-mode consumer.
- **Surfaces covered.** All five consumer builders. The two single-topic builders — base `ConsumerBuilder` and `TypedConsumerBuilder` — use the `ConsumerApi` poller above. The three wrapper consumers — `MultiTopicsConsumer`, `PartitionedConsumer`, `PatternConsumer` — are **not** `ConsumerApi` (their `receive()` yields topic-tagged wrapper messages), so they get a second poller over a distinct trait; see the wrapper-surface extension below.

## Wrapper-surface extension (multi-topic / partitioned / pattern)

The wrapper consumers (`MultiTopicsConsumer`, `PartitionedConsumer` — a type alias — and `PatternConsumer`) fan a single subscription across N child consumers; their `receive()` returns a **topic-tagged** wrapper message (`MultiTopicsMessage` / `PatternMessage` = `IncomingMessage` + originating topic) so the caller knows which child to ack against.
They are therefore not `ConsumerApi`, and the single-topic poller cannot drive them.

- **Second poller.** `spawn_wrapper_message_listener<R: WrapperReceiver>(receiver, listener)` (`crates/magnetar/src/consumer_listener.rs`), a sibling of `spawn_message_listener`, generic over the `WrapperReceiver` trait — an `async fn wrapper_receive() -> Result<(String, IncomingMessage), PulsarError>` plus `is_empty()` and `membership_changed()`. `MultiTopicsConsumer` (hence `PartitionedConsumer`) and `PatternConsumer` implement it by delegating to their own `receive()`. One poller serves all three surfaces, exactly as `spawn_listener_loop` serves every `ConsumerApi`.
- **Callback shape.** `WrapperMessageListener = Arc<dyn Fn(&str, &IncomingMessage) + Send + Sync>` — the originating **topic** is the extra first argument (mirroring Java `Message#getTopicName()`), so the callback can route an explicit ack via the wrapper's topic-keyed `ack(topic, id)`. Same contract otherwise: sequential, in order, no auto-ack, clean shutdown.
- **Builder surface.** `MultiTopicsConsumerBuilder` / `PartitionedConsumerBuilder` / `PatternConsumerBuilder` each gain `message_listener(WrapperMessageListener)` + `subscribe_with_listener()`. The terminal subscribes, then spawns the wrapper poller, returning the same `MessageListenerHandle`. The `PartitionedConsumerBuilder` forwards its listener onto the underlying `MultiTopicsConsumerBuilder`.
- **Pattern-child inheritance — decided: children discovered after subscribe inherit the listener.** This matches Java: `MultiTopicsConsumerImpl` / `PatternMultiTopicsConsumerImpl` own a single per-consumer listener executor and create every child — initial or later-discovered — with its own `messageListener` set to `null` (`getInternalConsumerConfig`, verified at `pulsar-client/.../MultiTopicsConsumerImpl.java:715`), routing all delivery through the parent. A naive "re-snapshot the child set on the next `receive()`" does **not** achieve this: a poller parked in `select_all` over the old child set never observes a child added while it waits. The fix is a **membership-change `Notify`** on each wrapper's `Inner`, signalled on every child add (`MultiTopicsConsumer::add_topic`, and each addition in `PatternConsumer::update`). The poller **races** its in-flight `wrapper_receive()` against `membership_changed()` (`tokio::select!`, `biased`); when a child joins while the poller is parked, the membership signal wins, the stale receive is dropped (cancel-safe — unpopped messages stay queued), and the next iteration re-snapshots and drains the new child. An empty wrapper (a pattern with no current match) parks on the membership signal instead of spinning on the empty-set error. `Notify` is not a channel, so ADR-0003 holds; it stores one permit, so an add that races a wait is not lost. The membership-change signal also benefits pull-mode callers indirectly (the poller is the only consumer of it today) and is exercised end-to-end against a live broker in `crates/magnetar/tests/e2e_wrapper_message_listener.rs` (a topic created after subscribe reaches the inherited listener) plus a deterministic façade unit test in `consumer_listener.rs`.

## Consequences

- **Easier.** Idiomatic push consumers matching the Java client; the callback type and lifetime semantics are consistent with `TableView::listen`, so the surface is learnable from one precedent.
- **Cost.** A new facade module (`consumer_listener.rs`) and two builder terminals. One `tokio::spawn`ed task per push consumer (same cost as a `TableView`).
- **No proto change.** `git diff --stat` shows no `crates/magnetar-proto/` edits — the cross-runtime parity is carried by the tokio + moonpool + differential layers (`receive()` itself is already proto-tested), so the ADR-0024 proto-unit layer (a) is N/A for this runtime-only feature.
- **Incompatible with** mixing pull and push on one consumer — by construction, since the consumer is consumed by the poller. This matches Java's documented rule.
- **No deferred residual.** Multi-topic / partitioned / pattern push delivery shipped via the wrapper-surface extension below.

## References

- `crates/magnetar/src/consumer_listener.rs` — `MessageListener`, `MessageListenerHandle`, `spawn_listener_loop` / `spawn_message_listener`.
- `crates/magnetar/src/builders.rs` — `ConsumerBuilder::message_listener` / `subscribe_with_listener`.
- `crates/magnetar/src/typed.rs` — `TypedMessageListener`, `TypedConsumerBuilder::message_listener` / `subscribe_with_listener`.
- `crates/magnetar/src/table_view.rs:240,584` — the `TableView::listen` / `spawn_drain` pattern this mirrors.
- `crates/magnetar/src/consumer_listener.rs` — `WrapperMessageListener`, `WrapperReceiver`, `spawn_wrapper_message_listener` (the wrapper-surface poller + membership-race inheritance).
- `crates/magnetar/src/multi_topics.rs`, `crates/magnetar/src/partitioned_consumer.rs`, `crates/magnetar/src/pattern_consumer.rs` — wrapper builders' `message_listener` / `subscribe_with_listener` + the `WrapperReceiver` impls + the membership-change `Notify`.
- `pulsar-client/src/main/java/org/apache/pulsar/client/impl/MultiTopicsConsumerImpl.java:715` (`getInternalConsumerConfig` sets child `messageListener` to `null`) and `PatternMultiTopicsConsumerImpl.java` — the Java parent-owns-the-listener model the inheritance decision matches.
- [ADR-0003](0003-no-channels-rule.md) (no channels), [ADR-0004](0004-sans-io-protocol-core.md) (proto zero I/O), [ADR-0011](0011-clock-injection-sans-io.md) (clock injection), [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) (cross-runtime test policy), [ADR-0025](0025-engine-trait-task-and-timer-primitives.md) (both engines schedule on tokio), [ADR-0038](0038-split-connection-mutex.md) (lock ordering).
- `docs/follow-ups.md §7` — the follow-up this wrapper-surface extension closes (multi-topic / partitioned / pattern push delivery).
