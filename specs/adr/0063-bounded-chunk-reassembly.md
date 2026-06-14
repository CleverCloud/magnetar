# ADR-0063 — Bound consumer-side chunk reassembly

- **Status**: Accepted
- **Date**: 2026-06-14
- **Decider**: Florentin Dubois
- **Tags**: consumer, chunking, pip-37, dos, memory-bound, java-parity

## Context

PIP-37 chunked messages are reassembled on the consumer side: `ConsumerState::deliver` buffers each chunk of a logical message — keyed by the broker-supplied chunk UUID — in `ConsumerState::chunk_reassembly: HashMap<String, ChunkBuffer>` ([`crates/magnetar-proto/src/consumer.rs`](../../crates/magnetar-proto/src/consumer.rs)) until every chunk arrives, then surfaces one logical `IncomingMessage`.

Before this ADR the map was **unbounded** along two axes:

- **Breadth.** A buffer was inserted on first-chunk arrival and removed ONLY on full reassembly. A hostile/buggy broker streaming distinct-UUID first chunks that never complete grew the map without bound → OOM / DoS. There was no size cap, no oldest-eviction, and no expiry sweep — `Connection::poll_timeout` / `handle_timeout` never referenced `chunk_reassembly` at all, so even a stale buffer lived forever.
- **Depth.** `deliver` validated `chunk_id` against `total` (`chunk_id < 0 || chunk_id >= total`) but never bounded `total` (`num_chunks_from_msg`) itself. A broker advertising a huge `total` could stream many distinct chunk_ids into ONE buffer (`ChunkBuffer::chunk_payloads: HashMap<i32, Bytes>`), blowing up a single buffer independent of any buffer-count cap.

The Java client bounds all of this: `maxPendingChunkedMessage = 10`, `autoAckOldestChunkedMessageOnQueueFull = false`, `expireTimeOfIncompleteChunkedMessageMillis = 60000` (`ConsumerConfigurationData.java:253,261,268`), enforced via `pendingChunkedMessageUuidQueue` + `removeOldestPendingChunkedMessage()` + `removeExpireIncompleteChunkedMessages()` (`ConsumerImpl.java:1631-1639,3125-3148`). Java additionally gates buffer **creation** on `chunkId == 0` and discards out-of-order / orphan chunks.

Alternatives considered:

- **Expose `total` / buffered-bytes caps as user knobs.** Rejected — Java exposes no such knob; these are pure safety floors, not tuning parameters. Surfacing them invites operators to mis-size them.
- **Sweep stale buffers only inside `handle_timeout` without surfacing the deadline through `poll_timeout`.** Rejected — the driver would never schedule a wake, so the bound would fire only opportunistically on an unrelated tick. That makes the eviction non-deterministic and seed-divergent under the moonpool engine (violating ADR-0024 / ADR-0036 reproducibility).

## Decision

Bound chunk reassembly in `magnetar-proto` (one change covers both engines), matching the Java defaults and semantics:

- **Breadth cap** — `ConsumerState::max_pending_chunked_message` (default `10`, `0` disables). A FIFO insertion-order index `chunk_reassembly_order: VecDeque<String>` mirrors Java's `pendingChunkedMessageUuidQueue`. When a genuine first chunk pushes the map past the cap, `remove_oldest_pending_chunked_message` evicts the front (oldest) buffer, removing the uuid from BOTH the map and the order index atomically.
- **Eviction / expiry ack policy** — `auto_ack_oldest_chunked_message_on_queue_full` (default `false`). `false` drops the partial without acking (the broker eventually redelivers the whole message); `true` stages the partial's first-chunk id into `chunk_auto_ack_pending`, which `Connection::handle_timeout` drains and individually acks. Mirrors Java `removeChunkMessage(.., autoAck)`.
- **Expiry sweep** — `expire_time_of_incomplete_chunked_message` (default `60s`, `None` disables). `ChunkBuffer::received_at` is stamped from the injected `now` (never `Instant::now()` — ADR-0011). `ConsumerState::next_chunk_expiry_deadline` surfaces the earliest `received_at + expire` through `Connection::poll_timeout`'s per-slot min-loop so the driver schedules a deterministic wake; `sweep_expired_chunks` (called from `Connection::handle_timeout`) drops every expired buffer off the same injected `now`.
- **Buffer-creation gate** — only a genuine first chunk (`chunk_id == 0`) may create a buffer; a straggler non-first chunk for an unknown/evicted uuid is dropped, never fabricating a corrupt buffer from non-first metadata. Mirrors Java's `chunkId == 0` gate.
- **Depth-axis safety floors** (compile-time constants, not user knobs):
  - `MAX_CHUNK_TOTAL = 10_000` — reject any chunk whose advertised `total` exceeds this, before it can pre-size `chunk_payloads` or the `0..total` reassembly loop. At Pulsar's default 5 MiB per-chunk wire limit that ceiling is ~48 GiB reassembled — far above any legitimate message — while rejecting the `total = i32::MAX` attacker.
  - `MAX_BUFFERED_CHUNK_BYTES = 128 MiB` — reject a chunk that would push the AGGREGATE buffered chunk-payload bytes (across every incomplete buffer) past this. This is the tight memory ceiling: total chunk-reassembly memory can never exceed 128 MiB regardless of how the bytes split across uuids or chunk_ids, versus the previous unbounded growth.

The three Java-matching knobs are threaded through `SubscribeRequest` (seeding `ConsumerState` in `Connection::subscribe`) and exposed on all five consumer builder surfaces (`ConsumerBuilder`, `TypedConsumerBuilder`, `MultiTopicsConsumerBuilder`, `PatternConsumerBuilder`, `PartitionedConsumerBuilder`) with snake_case names matching the Java semantics and defaults.

## Consequences

- **Easier**: a hostile/buggy broker can no longer OOM the client via chunk reassembly, on either axis. The bound is deterministic and seed-reproducible because the expiry deadline is surfaced through `poll_timeout`.
- **Java parity**: the default behaviour (`cap 10`, `60s` expiry, no auto-ack) matches the Java client exactly, including the `chunk_id == 0` creation gate.
- **Harder / cost**: out-of-order chunk arrival now requires the first chunk (`chunk_id == 0`) to arrive before any other chunk of the same message can open a buffer — a non-first orphan chunk is dropped. The broker dispatches chunks in order over a single connection, so this only affects pathological replay; the existing out-of-order test was updated to deliver chunk 0 first.
- **Incompatible with**: code that relied on a non-first chunk creating a reassembly buffer (the previous lenient behaviour, which was itself the corrupt-buffer DoS). No public-API break — the new knobs are additive with Java-matching defaults.

## References

- `crates/magnetar-proto/src/consumer.rs` — `ConsumerState` cap/eviction/expiry fields, `ChunkBuffer::received_at`, `remove_oldest_pending_chunked_message`, `next_chunk_expiry_deadline`, `sweep_expired_chunks`, and the `MAX_CHUNK_TOTAL` / `MAX_BUFFERED_CHUNK_BYTES` constants.
- `crates/magnetar-proto/src/conn.rs` — `poll_timeout` deadline surfacing + `handle_timeout` sweep + auto-ack drain; `subscribe` seeding.
- `crates/magnetar-proto/src/conn_types.rs` — `SubscribeRequest` knobs + defaults.
- `crates/magnetar/src/{builders,typed,multi_topics,pattern_consumer,partitioned_consumer,consumer_template}.rs` — the five builder surfaces.
- Apache Pulsar Java client: `ConsumerImpl.java:1576-1582,1631-1639,3094-3148`, `ConsumerConfigurationData.java:253,261,268`.
- [ADR-0011](0011-clock-injection-sans-io.md) — injected clock (the sweep reads `now`, never `Instant::now()`).
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the four-layer + e2e test coverage this change ships with.
- [ADR-0038](0038-split-connection-mutex.md) — the per-slot mutex the deliver/sweep paths take.
