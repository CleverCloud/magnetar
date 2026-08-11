# ADR-0080 — Configurable broker-operation retry policy

- **Status**: Accepted (amended by [ADR-0100](0100-close-cancelled-producer-open-before-retry.md), producer-open cancellation portion)
- **Date**: 2026-07-16
- **Decider**: Florentin Dubois
- **Tags**: resilience, retry, timeout, runtime, sans-io

> **Amendment (2026-08-21, [ADR-0100](0100-close-cancelled-producer-open-before-retry.md)).** Cancelling an opening PRODUCER is no longer local-only.
> Beside every local purge described below, it now writes one best-effort fire-and-forget `CommandCloseProducer` for the abandoned producer id whenever a `ProducerOpen` request for that handle is still pending or the producer is already `broker_ready`, and the slot is not already closed.
> Local-only cancellation was what stranded the broker-side `(topic, producer_name)` registration behind a timed-out open, and `ProducerBusy` being retryable here — the classification this ADR defines — then made the retry loop re-hit that zombie with a fresh producer id (issue #406).
> Cancelling an opening CONSUMER is unchanged.
> Everything else in this ADR stays in force: the retry policy and its defaults, the busy classification, the single per-operation deadline, per-generation request ownership, cancellation still owning all local correlation state, cancellation still being idempotent, and late broker replies still being ignored.

## Context

`SupervisorConfig` retries failed transports, but it does not cover a healthy connection whose broker temporarily rejects lookup, partition metadata, producer-open, or subscribe.
Magnetar 1.2.2 therefore had inconsistent behavior: lookup and partition metadata surfaced the first broker failure, while producer-open and subscribe used a fixed private policy inherited from ADR-0069.
That fixed policy retried `MetadataError`, `ServiceNotReady`, and `TopicNotFound` with a 2 s to 8 s schedule and eight re-issues.
It was not configurable, it treated Java-terminal `TopicNotFound` as retryable, and its roughly 54 s count-bound schedule could exceed the default 30 s `operation_timeout`.

The user-facing setup operation can span multiple commands.
For example, `ProducerBuilder::create` queries partition metadata, performs lookup and redirect dialing, then waits for `CommandProducerSuccess`.
Giving each stage a fresh timeout would multiply the advertised operation budget and make Tokio and Moonpool terminate at different points.

## Decision

### Separate operation retry from transport reconnection

Add `OperationRetryConfig` beside, not inside, `SupervisorConfig`:

```rust
pub struct OperationRetryConfig {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_retries: Option<u32>,
}
```

Defaults preserve the established cadence: 2 s initial backoff, 8 s maximum backoff, and `Some(8)` retries after the initial attempt.
`Some(0)` disables re-issues.
`None` removes the count cap, but the total `operation_timeout` still bounds the operation.
`ClientBuilder::operation_retry` exposes the policy on the façade.
The runtime clients also expose `with_operation_retry` for direct engine users, while the existing public `ConnectionConfig` fields remain source-compatible.

The retry policy is independent from transport supervision.
Broker-error retries do not consume `SupervisorConfig::max_attempts`, and reconnect attempts do not consume `OperationRetryConfig::max_retries`.

### Use an operation-specific compatibility allowlist

Lookup, partition metadata, producer-open, and subscribe retry:

- `MetadataError`;
- `PersistenceError`;
- `ServiceNotReady`;
- `TooManyRequests`.

Producer-open additionally retries:

- `ProducerBlockedQuotaExceededError`;
- `ProducerBlockedQuotaExceededException`;
- `ProducerBusy`.

Subscribe additionally retries:

- `ConsumerBusy`.

`ProducerBusy` remains terminal outside producer-open, and `ConsumerBusy` remains terminal outside subscribe.
`TopicNotFound`, authentication and authorization failures, schema errors, invalid topics, fencing, termination, and unknown codes remain terminal. The busy additions are deliberate Magnetar compatibility extensions required by issue #343; they are not claimed as Java 4.0.4 parity.

### Split provisional setup from established reattachment

Before the first successful attachment, a retryable producer-open or subscribe rejection removes the provisional proto handle and returns the exact broker error to the runtime client.
The runtime client owns the retry loop: it backs off, re-runs lookup and target resolution, and allocates a fresh provisional handle on the resolved connection.
This routing-aware path permits a retry to follow a redirect or broker-ownership move.

After a producer or consumer has attached at least once, retryable reattachment failures retain the established handle and emit the corresponding `*Transient` event.
The driver owns that lifecycle retry and reattaches the same handle after lookup.
Established reattachment uses the configured policy with an independent per-handle count; it does not consume the caller's completed setup-operation retry count.
Its successful consumer acknowledgement updates durable attachment state and releases gated flow without leaving an unowned `SubscribeAcked` event in the semantic queue.
A user-owned subscribe or seek keeps one stable logical waiter token while retry and reconnect replace its active wire `RequestId`.
Only the current active request can complete that token, so an older same-handle acknowledgement cannot satisfy the waiter or release flow, and a delayed retry keyed by the failed request id becomes a no-op after a newer subscribe generation supersedes it.
Completion remains durable across reset but is consumed only on a connected rebuilt session; dropping the waiter transfers the active or next rebuilt subscribe to flow ownership.
Producer and consumer attachment state both retain the active wire `RequestId`.
Only that generation may accept an acknowledgement or transient failure, terminalize the handle after lookup failure, or emit another retry; a reconnect rebuild or newer retry makes every older leg a no-op.
Established retry lookups run only after the reconnect handshake reaches `Connected` and are awakened when their generation is replaced, so a blackholed lookup cannot survive the request that authorized it.
A terminal broker error on the current established generation drains producer sends or marks the consumer terminal and wakes every parked operation before removing replay state.
Canceling a setup request removes its correlation, landed outcome, and queued success, failure, or broker-close attachment event, and late broker replies for that request are ignored.

### One operation context per public setup operation

`OperationDeadline` contains one provider-backed pinned timer and one mutable latest-broker-error slot allocated at the caller-visible entry point.
The timer includes partition metadata, PIP-145 topic-list snapshots, lookup, redirect dialing, retry sleeps, producer-open, and subscribe acknowledgement.
Composite façade operations reborrow the same timer and error slot across every child request.
Cleanup of already-opened children runs outside the setup deadline.

Tokio uses `tokio::time`.
Moonpool stores a type-erased sleep factory bound to its injected `TimeProvider`, so deterministic simulation never reads the host clock.
The deadline arm is biased first: an already-expired operation cannot enqueue another wire command.

Every retryable broker response replaces the operation context's previous diagnostic.
A successful intermediate retry does not clear that diagnostic, so a later stage or composite child that reaches the deadline still returns the newest broker code and message observed anywhere in the caller-visible operation.
Count-budget exhaustion returns the current broker error directly.
If the deadline expires before any broker error was observed, Magnetar returns the runtime timeout error.

### Cancellation owns all local correlation state

Dropping or timing out a request removes its waker, landed outcome, `pending_requests` entry, and lookup/partition registry slot.
Dropping or timing out an opening producer or consumer removes its pending request and handle state.
Cancellation is idempotent; late broker replies are ignored.
A provisional attachment retry drops its guard before backoff, removing the pending request and provisional handle before the next lookup creates a fresh one.
Detached established-reattachment legs stop when their retained handle disappears.

## Consequences

- Applications migrating from pulsar-rs can configure operation retries explicitly without confusing them with transport reconnection.
- Lookup and partition metadata recover the same transient broker conditions as producer-open and subscribe.
- `operation_timeout` is now an actual setup-operation deadline instead of only a connect/handshake budget.
- Public producer, partitioned-producer, consumer, multi-topic-consumer, partitioned-consumer, and pattern-consumer builders do not multiply the deadline across nested requests.
- Moonpool timing remains deterministic and Tokio/Moonpool retry counts remain 1:1.
- Existing exhaustive `ConnectionConfig` literals remain source-compatible because operation retry is installed on runtime clients rather than added as a public configuration field.
- ADR-0069's recoverable-versus-terminal receive gating remains binding.
  Its fixed error set, fixed count, and fixed backoff implementation are superseded by this ADR.

## Verification

- Proto: operation-specific busy classification, retry arithmetic, provisional-versus-established event routing, final-error preservation, and cancellation-capacity tests.
- Tokio and Moonpool: matched lookup retry, partition-metadata retry, deadline-during-backoff, ownership-move redirect, producer-open give-up, subscribe give-up, stale-generation terminalization, and blackholed-generation cancellation tests.
- Differential: configured producer-open and subscribe give-up equivalence with exact terminal broker codes.
- End-to-end: a frame-aware gate rejects the first producer-open with `ProducerBusy`, then forwards the configured retry to a real Pulsar broker.

## References

- [Issue #343](https://github.com/CleverCloud/magnetar/issues/343)
- [ADR-0011](0011-clock-injection-sans-io.md) — injected clocks.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime coverage.
- [ADR-0052](0052-initial-connect-timeout-retry.md) — existing `operation_timeout` connect semantics.
- [ADR-0069](0069-bounded-transient-open-retry-and-recoverable-receive-gating.md) — superseded fixed retry policy; retained receive gating.
