# ADR-0151 — Withdraw what a given-up send and a closed producer still have staged

- **Status**: Accepted
- **Date**: 2026-11-21
- **Decider**: Florentin Dubois
- **Tags**: producer, send-timeout, batching, close, drain-gate, state-machine, tokio, moonpool, sans-io
- **Amends**: [ADR-0106](0106-reattach-broker-closed-producer-in-place.md)

## Context

A producer stages the frame of every send in its slot's `outbound` queue, and the connection drains the queue into the wire buffer only while the connection is `Connected` and the slot's `broker_ready` gate is open (ADR-0038, ADR-0106).
A batching producer holds a message in its batch container until a limit or a flush turns the container into one frame.
Three paths told the caller that a message failed, or told the broker that the producer was gone, and then wrote the message anyway.

- **A send that timed out behind the gate.**
  `ProducerState::drain_timed_out_sends` removed the expired `OpSend` from `pending` and left its frame in `outbound`.
  While the gate is shut (a broker-initiated `CommandCloseProducer` being re-attached in place, or a reset being rebuilt) the frame waits, and the broker's `CommandProducerSuccess` then opens the gate and the drain writes it.
  The caller had been resolved with `SendError(-1, "send timeout")` and a retry carries a new sequence id, so broker deduplication cannot catch the duplicate.
  ADR-0106 recorded this as an accepted "at-least-once" shape; it was a duplicate-publication path, and Java does not have it: `failPendingMessages` removes the failed op from `pendingMessages` and `resendMessages` replays only what is still there.
- **A batched send that timed out inside the container.**
  The same sweep removed the member's op and left its message in `batch.messages`.
  A batching producer with no publish delay (the builder's default) keeps a message below every limit until a flush, so after `send_timeout` the next flush published a message whose caller had already been told it failed.
  Java discards the container on a send timeout (`failPendingBatchMessages` → `batchMessageContainer.discard`).
- **A producer closed with frames still staged.**
  `Connection::close_producer` encoded `CommandCloseProducer` straight into the connection buffer while the slot's staged `CommandSend` frames stayed behind it, so the wire carried the close and then the publishes.
  By a reading of the broker source (not re-verified against a running broker), the broker answers such a send with `PersistenceError "Producer is closed"` before it has removed the producer, and closes the whole connection, with every other producer and consumer on it, once it has.
  A `CommandProducerSuccess` for an open sent before the close made it worse: the arm did not look at `closed`, so it replayed the relocated snapshots, re-staged the pending frames and reopened the gate of a producer the broker had been told to drop.
  Java writes a send when it is issued, so its close always follows its sends, and its `handleProducerSuccess` sends nothing for a closing producer.

The `broker_ready` gate was meant to keep a not-ready producer silent; none of the three paths checked that a frame it was about to write still belonged to a send, or a producer, the client wanted.

## Decision

The sweep takes back what an expired send would still put on the wire, and a close writes or drops what the producer staged before it.

- **`drain_timed_out_sends` drops the staged frame of every expired send.**
  The frame is found by sequence id, which names every chunk of a chunked send, so all chunks go; the sends that have not expired keep their frames, in order.
  A flushed batch frame is keyed by the lowest sequence id of its members, which is the first member's id, and the first member is the first to expire, so the frame goes with it.
  The frame carries every member's payload and cannot be cut down, so the members behind it that have not expired yet stay in `pending` with no frame and fail at their own deadline; a frame already written is out of reach and is left to the broker, as before.
- **`drain_timed_out_sends` discards the unflushed batch container as a unit.**
  When an expired op is a member of the container (`batch_lowest` equals the container's lowest sequence id), the container is cleared and every other member is returned with the expired ones, so the connection's sweep fails them all with the send-timeout error and releases their reservations.
  This is Java's behavior, and the members that had not expired fail early, as they do there.
  Container members are the tail of `pending` (a message that cannot join the batch flushes it first) and the expired ops are a prefix, so once one of them was a member everything that remains in `pending` is a member too; the index is cleared with it.
- **`close_producer` drains before it closes.**
  `Connection::close_producer_inner` runs `drain_producer_outbound` before it encodes the close, so a send staged while the gate was open is written ahead of the close, as in Java, and the broker's receipt for it still resolves it.
  `ProducerState::close` then drops whatever is still staged: the drain writes nothing while the gate is shut, and nothing while the connection is not `Connected`, which includes the window between a `CommandAuthChallenge` and its answer, when every slot's own gate is open.
  A send dropped here stays in `pending` and fails with the rest when the broker acknowledges the close.
- **The `ProducerSuccess` arm replays nothing for a closed producer.**
  When the slot is closed it replays no pending frames and no snapshots and leaves the gate shut, and it fails the slot's snapshot bucket with `OpOutcome::Terminal` and the reason `producer closed before the send was acknowledged`, as `rebuild_producers` does for a producer it does not re-attach (ADR-0135).
  It fails the bucket there although the answer to the close would not strand it: an acknowledgement and a `CommandError` both reach `fail_sends_of_closed_producer` (the `Success` and `Error` arms), so failing it at the re-attach acknowledgement keeps the arm consistent with `rebuild_producers` and releases the relocated bytes earlier.
  The answer to the close, which arrives afterwards, finds no bucket and resolves nothing twice.
  The rest of the arm is unchanged: the open request is cleared, the name and `last_sequence_id` are recorded, the retry budget is reset, and `ProducerReady` is still queued.
  Nothing else opens the gate, so no drain-side check for `closed` is added: it could not be reached.
- The batch container is left alone at a close.
  `closed_producer_batch_deadline` pins that an unflushed batch stays in place until the close acknowledgement, a reset, the send-timeout sweep or `fail_all_pending` discards it.
- No new public API, no wire change, no new dependency.

## Consequences

- A caller told `send timeout` never sees that message published by a later drain or flush, and the staged payload no longer stays pinned outside the memory-limit accounting after its reservation was released with the op.
- A message in the batch container that outlives `send_timeout` now fails together with the other messages of its container, including ones that had not expired; the broker never sees any of them.
- The trailing members of a flushed batch frame that was dropped behind the gate fail at their own deadline, up to the spread of their enqueue times after the first member; failing them with the first would need the sweep to walk `pending` past the expired prefix, and the bound is the batch window.
- A producer's staged sends are written ahead of its close and are receipted as usual; the sends it staged behind a shut gate, or during an authentication challenge, are dropped and fail at the close acknowledgement (a close the broker rejects with `CommandError` fails them the same way, through `fail_sends_of_closed_producer`).
  That matches the documented contract that `flush()` before `close()` is what gives a trailing send at-least-once delivery.
- Not addressed, and not introduced here: `handle_timeout` flushes the batches that are due and drains the staged frames before it runs the `send_timeout` sweep. When `batching_max_publish_delay` is at least `send_timeout` and both fall due on the same tick with the gate open, the batch is written and its members are then failed with `send timeout`; the order of the two sweeps is unchanged by this decision.
- `close_producer` now drains every producer's staged frames, not only the closing one's, before it encodes the close.
  The frames land in the connection buffer in the order `poll_transmit` would have put them, ahead of the close; no relative order between commands that were already encoded changes.
- A broker that unloads a producer while its sends are timing out no longer produces a duplicate on the re-attach.
  ADR-0106's statement that "a timed-out publish's staged frame is still flushed once the gate reopens" no longer holds; its replay of a publish already receipted-pending at the close, and its batched-frames-on-the-wire residual, are unchanged.
- A real broker reaches the batch-container path deterministically, and `crates/magnetar/tests/e2e_batched_send_timeout.rs` covers it: a batching producer without a publish delay, one send that times out in the container, then a `flush()`. The other paths are asserted at the four ADR-0024 layers and have no end-to-end test; tests for them look possible and are not written here: holding the rebuild's `CommandProducerSuccess` after a cut with the frame proxy (`hold_broker_frames_from`, then `cut()`, as `e2e_producer_close_across_reconnect.rs` holds `CommandConnected`) orders a send timeout or a close before the re-attach acknowledgement; the in-place re-attach after an unload would need the proxy to hold frames on an established connection; and a send staged just before `close()` may be orderable on a current-thread runtime, where `Producer::send` stages the frame without yielding and `close()` encodes the close in its first poll.

### ADR-0024 test layers

(a) `magnetar-proto/tests/staged_frames_lifecycle.rs`, with the state-machine and reservation cases in `producer.rs` (`drain_timed_out_sends_*`) and `conn.rs` (`memory_limit_release_tests`); (b) `magnetar-runtime-tokio/tests/staged_frames_lifecycle.rs`; (c) `magnetar-runtime-moonpool/tests/staged_frames_lifecycle.rs`; (d) `magnetar-differential/tests/staged_frames_lifecycle_equivalence.rs`.
The multi-member container, the chunked send, the flushed batch frame and the authentication-challenge close are asserted at the proto layer only: the engine layers drive the same `Connection`, so their single-member scenarios execute the same lines.

## References

- `crates/magnetar-proto/src/producer.rs` — `ProducerState::drain_timed_out_sends`, `ProducerState::close`, `OpSend::batch_lowest`.
- `crates/magnetar-proto/src/conn.rs` — `Connection::close_producer_inner`, the `Type::ProducerSuccess` arm, `fail_sends_of_closed_producer`, `drain_producer_outbound`.
- [ADR-0106](0106-reattach-broker-closed-producer-in-place.md) — the in-place re-attach and its "at-least-once across the re-attach" consequence, which this amends.
- [ADR-0111](0111-share-one-memory-limit-controller-per-client.md) — the reservation release points; a dropped op still releases exactly once.
- [ADR-0135](0135-fail-the-sends-of-a-producer-closed-across-a-reset-at-the-rebuild.md) — the snapshot bucket of a closed producer, failed at the rebuild or by the close acknowledgement.
- [ADR-0145](0145-fail-every-member-of-a-batch-the-broker-rejects.md) — `batch_lowest`, the key this ADR reuses to find a batch's members.
