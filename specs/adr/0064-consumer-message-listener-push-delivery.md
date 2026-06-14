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
- **Surfaces covered.** The two single-topic builders — base `ConsumerBuilder` and `TypedConsumerBuilder`. The multi-topic / partitioned / pattern wrapper consumers are **deferred** (`docs/follow-ups.md §7`): they are not `ConsumerApi` (their `receive()` yields topic-tagged wrapper messages), so they need a distinct wrapper-message poller — tracked, not silently omitted.

## Consequences

- **Easier.** Idiomatic push consumers matching the Java client; the callback type and lifetime semantics are consistent with `TableView::listen`, so the surface is learnable from one precedent.
- **Cost.** A new facade module (`consumer_listener.rs`) and two builder terminals. One `tokio::spawn`ed task per push consumer (same cost as a `TableView`).
- **No proto change.** `git diff --stat` shows no `crates/magnetar-proto/` edits — the cross-runtime parity is carried by the tokio + moonpool + differential layers (`receive()` itself is already proto-tested), so the ADR-0024 proto-unit layer (a) is N/A for this runtime-only feature.
- **Incompatible with** mixing pull and push on one consumer — by construction, since the consumer is consumed by the poller. This matches Java's documented rule.
- **Deferred residual.** Multi-topic / partitioned / pattern push delivery (`docs/follow-ups.md §7`).

## References

- `crates/magnetar/src/consumer_listener.rs` — `MessageListener`, `MessageListenerHandle`, `spawn_listener_loop` / `spawn_message_listener`.
- `crates/magnetar/src/builders.rs` — `ConsumerBuilder::message_listener` / `subscribe_with_listener`.
- `crates/magnetar/src/typed.rs` — `TypedMessageListener`, `TypedConsumerBuilder::message_listener` / `subscribe_with_listener`.
- `crates/magnetar/src/table_view.rs:240,584` — the `TableView::listen` / `spawn_drain` pattern this mirrors.
- [ADR-0003](0003-no-channels-rule.md) (no channels), [ADR-0004](0004-sans-io-protocol-core.md) (proto zero I/O), [ADR-0011](0011-clock-injection-sans-io.md) (clock injection), [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) (cross-runtime test policy), [ADR-0025](0025-engine-trait-task-and-timer-primitives.md) (both engines schedule on tokio), [ADR-0038](0038-split-connection-mutex.md) (lock ordering).
- `docs/follow-ups.md §7` — deferred multi-topic / partitioned / pattern push delivery.
