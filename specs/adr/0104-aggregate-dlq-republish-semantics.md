# ADR-0104 — Define aggregate DLQ republish semantics

- **Status**: Accepted (amends [ADR-0037](0037-multi-topics-pattern-consumer-pass-2-lift.md), the aggregate-DLQ surface only)
- **Date**: 2026-08-25
- **Decider**: Florentin Dubois
- **Tags**: consumer, multi-topics, partitioned, dead-letter, concurrency, cancellation

## Context

[ADR-0037](0037-multi-topics-pattern-consumer-pass-2-lift.md) lifted `republish_dead_letters` into the engine-generic `ConsumerApi` and made it available through `MultiTopicsConsumer` and its `PartitionedConsumer` alias.
The aggregate façade now needs an exact orchestration contract.

The implementation snapshots an `Arc<Vec<NamedConsumer<C>>>`, releases the collection lock, and awaits each child operation in turn.
That shape fixes traversal order and avoids holding the membership lock across I/O, but it does not isolate child handles from every concurrent action.
In particular, a cloned child consumer shares its runtime state, and `remove_topic` closes that shared child after removing it from current membership.

Concurrent aggregate calls also take separate snapshots and enter child operations independently.
The runtime currently destructively drains each child's buffered dead letters before republishing them, but the aggregate coordinator owns neither that drain nor any cross-call deduplication mechanism.
Its contract must not promote a runtime implementation property into a stronger aggregate guarantee.

## Decision

`MultiTopicsConsumer::republish_dead_letters`, including calls through the `PartitionedConsumer` alias, has the following contract:

1. **Per-call membership snapshot.** Each call independently snapshots the current child list.
   The snapshot's membership and vector/topic order are fixed for that call.
   Children added after the snapshot do not enter the operation.
2. **Removal race.** Removing a snapshotted child does not remove it from the call's traversal.
   `remove_topic` may nevertheless close the shared child handle, so the later or in-flight operation for that child may observe the close and fail according to the runtime.
3. **Lock release before I/O.** The collection mutex is released before the first child future is awaited and remains unheld throughout child work.
4. **Sequential deterministic traversal.** Children are invoked one at a time in snapshot vector/topic order through `ConsumerApi::republish_dead_letters`, all with the caller's one shared producer destination.
5. **Saturating count.** Successful child counts are combined with `usize::saturating_add`; overflow returns `usize::MAX` rather than wrapping or panicking.
   An empty snapshot returns `Ok(0)`.
6. **First error with topic context.** The first child error stops traversal immediately and is wrapped with that child's topic.
   Children after the failing child are not started.
7. **Partial progress.** Publications and acknowledgements completed by earlier children are not rolled back when a later child fails.
8. **Cancellation.** Dropping or aborting the aggregate future cancels the in-flight child future and prevents later children from starting.
   Work completed by earlier children remains completed.
9. **Concurrent calls.** Aggregate calls are not serialized.
   Each snapshots membership independently and may overlap on the same shared child handles.
   Per-child counts and outcomes follow the runtimes' existing destructive-drain behavior; the aggregate layer adds no cross-call deduplication guarantee.
10. **Publish before ACK remains unchanged.** This decision changes no runtime DLQ behavior.
    Each runtime child operation still confirms replacement publication before acknowledging the original message.

## Consequences

- Aggregate behavior is deterministic within one snapshot while allowing independent calls to overlap.
- Callers can rely on saturating arithmetic, first-error topic context, and retained partial progress.
- A membership snapshot isolates the traversed list, not the liveness of the shared child handles in that list.
- Callers that require aggregate-call serialization or a stronger deduplication policy must provide it outside `MultiTopicsConsumer`.
- The focused helper tests cover ordering and partial progress, lock release and membership snapshotting, cancellation, concurrent overlap, and the `usize::MAX` saturation boundary without pretending to model runtime drain deduplication.
- Tokio, Moonpool, and differential runtime tests continue to own the destructive-drain and publish-before-ACK behavior; this ADR does not alter those implementations or their tests.

### Amends ADR-0037

ADR-0037 remains binding for the `ConsumerApi` lift, the associated producer type, broker metadata dispatch, and all generic façade builders.
Its aggregate-DLQ surface is amended only by the orchestration contract above.

## References

- [`crates/magnetar/src/multi_topics.rs`](../../crates/magnetar/src/multi_topics.rs) — aggregate coordinator, public contract, and focused helper tests.
- [`ARCHITECTURE.md` § "DLQ + retry-letter"](../../ARCHITECTURE.md#dlq--retry-letter) — runtime and aggregate flow.
- [ADR-0037](0037-multi-topics-pattern-consumer-pass-2-lift.md) — engine-generic consumer and aggregate façade lift.
