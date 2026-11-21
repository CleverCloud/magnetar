# ADR-0145 — Fail every member of a batch the broker rejects

- **Status**: Accepted
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: producer, batching, send-error, memory-limit, state-machine, tokio, moonpool, sans-io
- **Amends**: [ADR-0111](0111-share-one-memory-limit-controller-per-client.md)

## Context

A batching producer mints one sequence id and one `OpSend` per logical message (`ProducerState::add_to_batch`), then `flush_batch` folds the whole container into ONE `CommandSend` carrying `sequence_id = lowest` and `highest_sequence_id = highest`.
Nothing recorded which ops rode in which frame.

The broker answers such a frame with one reply.
A success is a `CommandSendReceipt` whose `highest_sequence_id` carries the range, and the `SendReceipt` arm of `Connection::handle_frame` fans it out over `[lowest, highest]`, resolving every member.
A failure is a `CommandSendError`, which has no range field.
The broker validates a batch frame against its highest sequence id (per the review's reading of the broker source: `ServerCnx` routes a frame carrying `highest_sequence_id` to `publishMessage(…, lowest, highest, …)`, which validates through `checkAndStartPublish(producerId, highestSequenceId, …)`), so every rejection before the entry is persisted — `PersistenceError` for a closed producer, `ChecksumError`, `MetadataError` ("Messages must be encrypted") — names the HIGHEST id, and a persistence failure names `max(highestSequenceId, sequenceId)`.

The `SendError` arm called `ProducerState::apply_send_error` once, and that removed only the op whose id the broker named.
The entry was not persisted, so no receipt follows for the other members, and each of them kept its op, its future and its reservation against the client-wide memory budget until the 30 s `send_timeout` (forever with `send_timeout: None`), then failed with the synthetic `send timeout` error instead of the broker's.
A batching producer without encryption on a topic with `encryption_required` hits this on every batch: N-1 of every N sends hang.
Java resolves the whole batch operation on the error (`ProducerImpl.recoverNotAllowedError` matches the highest sequence id of the batch op and completes it).

Only the broker error names a single id, so the missing piece is the membership of a frame, not the id the broker chose.

## Decision

`OpSend` gains a crate-private `batch_lowest: Option<SequenceId>`: the lowest sequence id of the batch frame the publish rides in, which `add_to_batch` copies from `BatchContainer::lowest_sequence_id` when it mints the op, and which is `None` for a single or chunked publish.
Every member of one batch shares the value, and it is the `sequence_id` the flushed `CommandSend` carries, so the frame is named before it exists and the record dies with the ops it describes.

`ProducerState::apply_send_error` now returns every removed op, oldest first, instead of at most one: the op the broker names and every other pending op with the same `batch_lowest`.
The `SendError` arm resolves each one exactly as it resolved the single op before — outcome, waker, observational event, then the op drops and releases its reservation — and adds the number of failed ops to `total_send_failed`.
Every member carries the broker's code and message.

- An op that is no longer pending (already timed out, or never sent) is not resurrected: members are collected from the live queue, so a member the send-timeout sweep removed earlier is not failed twice and its reservation is not released twice.
- A single or chunked publish has no `batch_lowest` and fails alone, as before.
- The tokio and moonpool engines read the outcomes the connection records; neither handles a `CommandSendError` itself, so the change is in `magnetar-proto` only and both engines get it identically.
- No wire change, no new dependency, and no change to the `magnetar` façade.
  `ProducerState::apply_send_error` is `pub` in the internal `magnetar-proto` crate and its return type changes from `Option<(OpSend, i32, String)>` to `Vec<(OpSend, i32, String)>`, recorded in `CHANGELOG.md` the way ADR-0111 recorded its earlier change to the same signature; its one caller is `Connection::handle_frame`, and no source file outside `producer.rs` and `conn.rs` names it.
  `OpSend::batch_lowest` is `pub(crate)`, as `OpSend`'s reservation already is.

## Consequences

- A broker error for a batch frame fails every member of the batch at once, with the broker's error, and releases all of their bytes against the client-wide `memory_limit` at that moment instead of at the send timeout.
- `ClientError::SendRejected` reaches every sender of a rejected batch, so a caller can tell an encryption or checksum rejection from a timeout.
- Cost: two passes over the producer's pending queue to find the frame's members, then one positional removal per member, the same per-member cost as the `CommandSendReceipt` fan-out through `apply_receipt`.
- `docs/follow-ups.md` §22 names the cases in which an op the broker never answers keeps its budget; the batch rejection was never listed there and is not one now.
- Incompatible with: a caller that relied on the other members of a rejected batch staying pending until `send_timeout`, which the missing receipt made indistinguishable from a hang.
- Not changed here: the send-timeout sweep pops only the op of a batch member that is still in the unflushed container, leaving its payload in the container for the next flush; that is a separate defect with its own fix.

### ADR-0024 test layers

(a) `magnetar-proto/src/conn.rs` `memory_limit_release_tests::send_error_for_a_batched_frame_fails_every_member_of_the_batch`; (b) `magnetar-runtime-tokio/tests/client_wide_memory_limit.rs` `send_error_for_a_batched_frame_fails_every_member`; (c) the same test in `magnetar-runtime-moonpool/tests/client_wide_memory_limit.rs`; (d) `magnetar-differential/tests/batch_frame_send_error_equivalence.rs`.
End-to-end: in `crates/magnetar/tests/e2e_batch_send_error.rs` a frame proxy flips the last byte of a three-member batch's frame, the broker answers it with one `ChecksumError` naming the highest member, and every member must fail with it; the next batch on the same producer, forwarded untouched, resolves on its own receipts.
The recovery after a rejection is also pinned in layer (a), `memory_limit_release_tests::the_next_batch_after_a_rejected_one_resolves_on_its_own_receipt`.

## References

- `crates/magnetar-proto/src/producer.rs` — `OpSend::batch_lowest`, `ProducerState::add_to_batch`, `apply_send_error`, `take_pending`.
- `crates/magnetar-proto/src/conn.rs` — the `SendError` arm of `Connection::handle_frame`, next to the `SendReceipt` fan-out.
- [ADR-0111](0111-share-one-memory-limit-controller-per-client.md) — the release points of a publish's memory reservation; its entry "`CommandSendError`" now reads "`CommandSendError`, for every member of a batch frame".
- [ADR-0096](0096-reconnect-batch-and-durable-cursor-safety.md) — the other place a batch's per-member ops are resolved together (reset).
