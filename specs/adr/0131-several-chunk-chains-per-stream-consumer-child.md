# ADR-0131 — Hold several chunk chains per `StreamConsumer` child and fail the consumer on a chain its budget cannot hold

- **Status**: Accepted (amends [ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md) §Aggregate receive budget and concurrency: the single purpose-fenced chunk chain per child and the resynchronization answer to every receive-accounting failure) (amended 2026-11-20 — a child whose topic has ended abandons its chunk chains)
- **Date**: 2026-11-19
- **Decider**: Florentin Dubois
- **Tags**: scalable-topics, stream-consumer, chunking, pip-37, receive-budget, flow-control

## Context

The assignment-driven `StreamConsumer` reassembles chunked messages (PIP-37) inside `StreamReceiveState` ([`crates/magnetar-proto/src/stream_consumer.rs`](../../crates/magnetar-proto/src/stream_consumer.rs)).
Two properties of the first implementation turned ordinary broker behaviour into a permanent resynchronization loop, because nothing in a chunk chain is acknowledged until it completes and the broker therefore redelivers the same entries, in the same order, after every reconnect.

1. **One chain per child, and a permit fenced to it.**
   After the first chunk the child's only outstanding permit was a `FlowPurpose::ChunkContinuation { assembly }` bound to that chain.
   An entry that was not the next chunk of the chain failed: a complete message with `FlowPurposeMismatch`, the first chunk of another producer's message with `InvalidChunkFrame`.
   The ordinary consumer keeps an independent chain per uuid and a test pins that the chunks of two messages interleave (`interleaved_chunked_messages_are_independent`), and [ADR-0063](0063-bounded-chunk-reassembly.md) bounds how many it keeps.
2. **A budget failure answered like a malformed frame.**
   A chain reserves `buffered bytes + announced size` for its assembly and one `MAX_FRAME_SIZE` permit for the next chunk.
   At the default 16 MiB budget a single object may hold 8,322,936 bytes, so a chain whose first chunk plus announced size exceeds 3,080,056 bytes can never be assembled.
   That is every message a producer chunks at the broker's default 5 MiB `max_message_size`.
   The model reported the shortfall as `MessageTooLargeForBudget` (assembly) or `Exhausted` (continuation permit), and `child_loop` of both runtimes answered any `message_arrived` error with `request_resync`.
   The proposal behind [ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md) (D10 of [`feat-m1-hardened-stream-consumer.md`](../proposals/feat-m1-hardened-stream-consumer.md)) and `docs/pip-features.md` promise that a message that cannot fit produces a typed `MessageTooLargeForBudget` and that budget exhaustion returns a typed resource failure.

## Decision

- **Several chains per child.**
  `StreamReceiveState` keys a chain by `(child, uuid)` and holds at most `MAX_PENDING_CHUNK_CHAINS` per child, the ordinary consumer's `max_pending_chunked_message` default of 10.
  The cap exists because an assembly can be a few bytes, so the budget alone does not bound how many chains a broker may open; an eleventh concurrent chain is `InvalidChunkFrame` and requests resynchronization, as every other malformed delivery does.
  Eviction of the oldest chain, as the ordinary consumer does, is not adopted: it would drop a partial message the broker will not redeliver without a reconnect.
- **The permit follows the entry.**
  A permit costs the same `MAX_FRAME_SIZE` whatever it is used for, and the purpose check of each model transition (`message_arrived*`, `chunk_frame_buffered`, `chunk_message_arrived`, `discard_preallocated_arrival`) is unchanged and still rejects a mismatched call.
  `StreamReceiveState::accept_entry`, which alone knows which chain an entry belongs to, binds the child's permit to that chain, or to an ordinary message, through one private model transition that changes no reservation, before it calls the transition that applies.
  A complete entry, a batch, or the first chunk of a new chain therefore spends the permit as itself and leaves every other chain open.
- **A child that holds a chain is served first.**
  A chain reserves its next permit together with its assembly so that it can always finish.
  The model records the assemblies each child holds, and `arbitrate_flow` serves such a child before any sibling and grants it a continuation permit, so the capacity released when an entry between two chunks spends the permit cannot be taken by a sibling.
- **A chain that cannot fit alone is terminal.**
  `chunk_frame_buffered` returns `MessageTooLargeForBudget { required, limit }` when this child's chains plus the continuation frame exceed the data limit minus the authority headroom, which holds with nothing else in the ledger and so after every resynchronization; `required` includes the continuation frame.
  `message_arrived` of both runtimes closes the consumer with that error (`fail_closed`): `receive` and `next_event` return `StreamConsumerError::Failed` carrying the typed text, and no reconnect is scheduled.
  The remedy is a larger `receiver_budget`.
  A shortfall that disappears when siblings or retained messages release their reservations stays `Exhausted` and resynchronizes.
- **Unchanged.**
  Decompression and batch-assembly budget failures, an orphan non-zero chunk, a mismatched announced size or chunk count and a duplicate chunk id remain malformed deliveries that request resynchronization, and the `malformed-delivery` differential scenario keeps pinning that.

## Consequences

- Interleaved chunked messages from several producers on one segment are delivered, as they are on the ordinary consumer.
- A consumer whose budget cannot hold an announced message reports it once and stops, instead of hot-looping through reconnects that never deliver a message; messages already queued for `receive` are dropped with the fence and redelivered by the broker after the caller reopens the consumer with a larger budget.
- At the default budget no message chunked at the default `max_message_size` can be received through a `StreamConsumer`; the failure is now visible, and sizing the budget remains the caller's decision.
- Decompression and batch-assembly budget failures can still loop: their resynchronization contract is pinned by the `malformed-delivery` differential scenario and is left to a decision of its own.
- The cap can still loop: nothing expires a chain whose remaining chunks never arrive, so a child that holds ten such chains answers the first chunk of an eleventh with a resynchronization, and the broker replays the same eleven chains after it.
- A chain holds its assembly while it waits, so a child with open chains can hold budget that a sibling would otherwise use; the cap and the structural limit bound it.

## References

- `crates/magnetar-proto/src/stream_consumer.rs` — `StreamReceiveState`, `MAX_PENDING_CHUNK_CHAINS`, `chunk_frame_buffered`, `bind_flow_purpose`, `arbitrate_flow`.
- `crates/magnetar-runtime-tokio/src/scalable.rs`, `crates/magnetar-runtime-moonpool/src/scalable.rs` — `message_arrived`.
- `crates/magnetar-differential/tests/stream_consumer_advanced_equivalence.rs` — `interleaved_chunk_chain_does_not_resynchronize_forever_on_{tokio,moonpool}`, `unassemblable_chunk_chain_does_not_resynchronize_forever_on_{tokio,moonpool}`.
- [ADR-0063](0063-bounded-chunk-reassembly.md), [ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md), [`docs/pip-features.md`](../../docs/pip-features.md) §Aggregate receive budget.

## Amendment (2026-11-20): a child whose topic has ended abandons its chunk chains

- **A terminal child abandons its chains.**
  `StreamConsumerModel::observe_terminal` releases, with the flow permit, the assembly of every chain the child holds and counts each out of the child's pre-terminal reservations, before it looks at the drain or re-arbitrates flow.
  `StreamReceiveState::observe_terminal` calls it and drops the chunks those chains retained, and the end-of-topic arm of `child_loop` in both runtimes calls that.
  The broker sends nothing after the end of the topic, and the child hands over every entry it received before it reports it, so no chain that could still finish is abandoned; the broker reports the end of the topic only once every entry is acknowledged, so a chain reaches this point only after a cumulative acknowledgement of a later entry covered its first chunk.
  A chain whose first chunk is never acknowledged keeps the broker from reporting the end of the topic at all, so on a sealed segment it still blocks completion and the segment's descendants.
  Without it the unreleased assembly kept `complete_segment` failing with `SegmentNotComplete`, which the runtimes swallow, and the segment's descendants stayed blocked behind a parent that had nothing left to deliver.
- **Consequence that changes.**
  The cap can still loop on a live segment: only the end of the topic abandons a chain, and nothing expires one whose remaining chunks never arrive, so a child that holds ten such chains answers the first chunk of an eleventh with a resynchronization, and the broker replays the same eleven chains after it.
  The same open chain keeps an ownership drain of a non-terminal child, and a vector seek, waiting until the child closes or the aggregate resynchronizes; an expiry that abandons it on a timer, as the ordinary consumer's `expire_time_of_incomplete_chunked_message` does, is not implemented, and whether to add one remains an open decision.
- **References.**
  `crates/magnetar-proto/src/stream_consumer.rs` — `observe_terminal`; `crates/magnetar-runtime-tokio/src/scalable.rs`, `crates/magnetar-runtime-moonpool/src/scalable.rs` — `child_loop`; `crates/magnetar-differential/tests/stream_consumer_advanced_equivalence.rs` — `incomplete_chunk_chain_does_not_block_descendants_of_a_terminal_parent_on_{tokio,moonpool}`.
