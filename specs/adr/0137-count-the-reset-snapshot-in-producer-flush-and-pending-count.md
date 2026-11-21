# ADR-0137 — Count the reset snapshot in `Producer::flush` and `pending_count`

- **Status**: Accepted
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: producer, flush, reconnect, pending-count, lock-ordering, tokio, moonpool, sans-io
- **Amends**: [ADR-0038](0038-split-connection-mutex.md)

## Context

`Producer::flush` is documented as "wait for every in-flight send to be acknowledged by the broker" and as the call to make before `close()` for at-least-once semantics on the trailing sends.
Both engines implemented it as a loop on the producer's live `pending` queue: the tokio engine read `slot.state.lock().pending.len()` and the moonpool engine read `Connection::producer_pending_count`, which reads the same queue.
`Producer::pending_count` (documented as "queued and not yet acked by the broker") read the same queue, per slot.

`Connection::reset()` moves every replayable in-flight publish out of that queue into the connection's `in_flight_publish_snapshots` bucket.
The sends return to `pending` only when the broker acknowledges the producer's re-attachment (`CommandProducerSuccess`), whose handler replays them with their original sequence ids; their `CommandSendReceipt` follows.
For the whole window between the reset and that acknowledgement — the new session's handshake and the re-attachment round trip — every send the producer had in flight is owed a receipt and sits in no queue the engines read.
The supervisor resets only once the new socket is dialled, under the post-dial lock; during the backoff and the dial the sends are still in the live queue, which `flush` always counted.

A `flush` called inside the window therefore returned `Ok(())` at once, and one parked across the socket drop re-checked after the driver's post-`CONNECTED` wake, read `0`, and returned `Ok(())` too.
The documented recipe `flush().await` then `close()` (or process exit) could complete while publishes were unacknowledged: followed by exit the relocated publishes are lost with no signal, and followed by `close()` they meet the close path while still unacknowledged ([ADR-0135](0135-fail-the-sends-of-a-producer-closed-across-a-reset-at-the-rebuild.md)).
`pending_count` reported `0` for the same sends, and `flush_with_timeout` would have reported "with 0 sends still pending" after timing out on them.

The snapshot bucket belongs to `Connection`, not to the producer slot, so the per-slot read ADR-0038 gave the observability getters cannot see it.

## Decision

`Connection::producer_unacknowledged_count(handle)` returns the live queue (`producer_pending_count`) plus the relocated publishes (`in_flight_publish_snapshot_len`).
Both engines build `Producer::flush` and `Producer::pending_count` on it.

- `Producer::flush` loops on `producer_unacknowledged_count` and returns only when it reads `0`: neither a live nor a relocated send is left.
  The sends relocated by a reset are counted from the reset until the re-attachment is acknowledged, then in the live queue until their own receipt, so the number never drops while a receipt is owed.
- `Producer::pending_count` returns the same number, in both engines, so the façade's `ProducerApi::pending_count` and `PartitionedProducer::pending_count` agree with `flush`.
- `tokio::Producer::flush_with_timeout` reports the same number in its timeout message.
- The two reads happen under the connection lock, then the slot lock: the `ProducerSuccess` handler removes the bucket and reinstalls it into `pending` inside one connection-lock section, so a caller holding the connection lock sees every send in exactly one of the two places.
  Reading the slot first and the bucket afterwards would race that move and could read `0` for a send in transit.
- `ProducerStats::pending_queue_size` is unchanged: it is a per-slot snapshot, summed by `PartitionedProducer::aggregate_stats`, and reports the live queue.
  `pending_count` is therefore no longer equal to it while a reset holds sends in the bucket.
- `Connection::producer_pending_count` keeps its meaning (the live queue) and its doc now says so; no wire, public-type or driver change.

## Consequences

- `flush` waits through a reconnect for the sends the reset relocated, and resolves only after their replayed receipts.
  A flush that must not wait for an unavailable broker uses `flush_with_timeout`, whose timeout message now counts them.
- `Producer::pending_count` takes the global connection mutex, then the slot mutex, instead of the slot mutex alone.
  This narrows ADR-0038's "observability getters bypass the global lock" for this one getter and for `flush`'s probe: the order stays global → per-slot, the section is a counter read that never spans an `.await`, and `Producer::send` still takes only the slot mutex.
  The other getters (`stats`, `batch_len`, `last_sequence_id`, …) are untouched.
- A caller must not call `pending_count` while holding the connection mutex (the lock is not re-entrant); none of the workspace's call sites does.
- Wake-up is unchanged: a parked `flush` is woken by the driver's `driver_waker.notify_waiters()` after each inbound packet, which covers the `CONNECTED` frame, the re-attachment acknowledgement and every receipt.
  The driver's timer arm wakes `event_waker` but not `driver_waker`, so a flush whose last unacknowledged send is failed by the `send_timeout` sweep is re-checked at the next inbound packet; that predates this change for the live queue and is recorded in `docs/follow-ups.md` §32.
- Incompatible with: a caller that used `flush` to mean "the live queue is drained" and relied on it returning during a reconnect window.

### ADR-0024 test layers

(a) `magnetar-proto`: `unacknowledged_count_covers_the_send_across_reset_snapshot_and_replay` (`conn.rs`); (b) `magnetar-runtime-tokio`: `tests/producer_flush_reconnect.rs` and the `producer.rs` unit tests `flush_with_timeout_times_out_while_a_reset_holds_the_send_in_the_snapshot` / `pending_count_includes_the_send_a_reset_moved_into_the_snapshot`; (c) `magnetar-runtime-moonpool`: the twins of the same names; (d) `magnetar-differential/tests/producer_flush_reconnect_equivalence.rs`.
End-to-end: in `crates/magnetar/tests/e2e_flush_across_reconnect.rs` a TCP proxy in front of a real broker drops one publish, cuts the session and holds the broker's `CommandProducerSuccess` on the redial; `flush` must stay pending until the re-attachment and the replayed send are acknowledged.
A real broker reaches the path whenever a connection drops with a publish unacknowledged and a `flush` runs before the re-attachment completes.

## References

- `crates/magnetar-proto/src/conn.rs` — `Connection::producer_unacknowledged_count`, `producer_pending_count`, `in_flight_publish_snapshot_len`, `reset`, the `ProducerSuccess` arm.
- `crates/magnetar-runtime-tokio/src/producer.rs`, `crates/magnetar-runtime-moonpool/src/producer.rs` — `flush`, `pending_count`.
- [ADR-0038](0038-split-connection-mutex.md) — the lock order and the per-slot getters this narrows.
- [ADR-0135](0135-fail-the-sends-of-a-producer-closed-across-a-reset-at-the-rebuild.md) — the other consumer of the snapshot bucket, for a producer that was closed.
