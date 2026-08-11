# ADR-0112 — Compress and decompress a batched entry as one body

- **Status**: Accepted (amends [ADR-0107](0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md), its four-site refund list; amends [ADR-0044](0044-moonpool-message-crypto-bridge.md), its "no decompression step on moonpool" for batched entries)
- **Date**: 2026-10-08
- **Decider**: Florentin Dubois
- **Tags**: consumer, producer, batch, compression, flow-control, wire-format, java-parity, sans-io, issue-860

## Context

Issue #860 reports a magnetar `Reader` that delivers nothing over a topic a Java producer filled with compressed batches, and stalls for good with nothing logged.
Reproduced on `apachepulsar/pulsar:4.0.4`: `pulsar-perf produce -bm 20 -z LZ4` in, zero messages out of a `receiver_queue_size(1000)` Reader, and the broker reports the reader's `availablePermits` at `-1000`.
Uncompressed batches stream fine.

The issue's own hypothesis — a permit debited per entry rather than per member — is wrong.
`ConsumerState::classify_and_queue` debits `permit_balance` once per batch member, and `pop_message` credits the `consumed_since_flow` refund ledger once per popped member.

### The consumer split the compressed bytes

A Java producer compresses the WHOLE packed batch body: `BatchMessageContainerImpl.getCompressedBatchMetadataAndPayload` concatenates `[u32 BE size][SingleMessageMetadata][payload]` for every member, compresses the concatenation once, and stamps `uncompressed_size` with its pre-compression length.
A Java consumer reverses that in the same order: `ConsumerImpl.messageReceived` calls `uncompressPayloadIfNeeded` on the whole payload before `receiveIndividualMessagesFromBatch` splits it.

`ConsumerState::deliver` split first.
It read the codec's output as member sizes, one of its length guards fired, and the loop broke before queuing a single member.
Nothing was queued, so nothing was ever popped, so the `numMessagesInBatch` permits the broker charged for the entry were never refunded.
Once the loss passed half the receiver queue, `maybe_flow` could never fire again: the broker sat at `-receiver_queue_size`, the consumer at zero, and nothing logged why.

### The producer wrote a layout nobody could read

The tokio engine compressed every payload in `Producer::send`, before the state machine decided whether it would batch, and `ProducerState::flush_batch` then stamped a batch-level `compression` and `uncompressed_size` on a concatenation that nothing compressed.
`uncompressed_size` held the sum of the COMPRESSED member sizes.
Java `pulsar-client consume` read 0 of 100 messages from such a topic (`Lz4RawDecompressor` errors), against 100 of 100 for LZ4 without batching and for batching without compression.

That layout — called the **legacy layout** below — was not readable by magnetar either.
Measured on `origin/main` 15e6ed6: a two-member legacy LZ4 batch reaches the tokio `Consumer::receive` as two members, and each one fails with `decompress: decompressed size mismatch: got 25, expected 32`, because the post-pop decompression checks every member against the batch-level `uncompressed_size`.
The only reason the defect stayed invisible is that magnetar's own consumer could not read Java's batches either.
`access-logs-forwarder` produces with LZ4 and batching, so backlogs in the legacy layout exist in production.

### Why the codecs have to sit in the state machine

The split happens in `magnetar-proto`, so the consumer needs a codec there.
The producer needs one there too: whether a message joins a batch is decided by `ProducerState::can_add_to_batch` under the per-slot lock, from the current batch fill.
An engine that compressed "if the message will not batch" would read that fill, release the lock, compress, and take the lock again — a decision another sender can invalidate in between.
All four codec crates are in-memory transforms with no I/O, so `cargo run -p xtask -- check-no-io-deps` stays green.

Alternatives rejected:

- **Decompress in the engine after the pop.** The split runs before the pop; the engine never sees the entry whole.
- **An engine-side "will this batch?" predicate.** Racy, as above.
- **Keep the legacy members' post-pop decompression.** Measured above: it has never decoded one.

An independent review of the first cut (2026-10-08) found four more defects in the same area, all fixed by this ADR:

- the legacy decoder sized LZ4's output buffer from the whole entry budget and `Bytes::from` kept it, so a 13,417-byte member held 3,421,359 bytes for as long as it sat in the receiver queue;
- `deliver` treated only `num_messages_in_batch > 1` as a batch, while `flush_batch` stamps `Some(1)` on a one-message flush, so a magnetar consumer handed the `[u32][SingleMessageMetadata]` framing to the application as payload — silently, now also for a compressed one;
- a send that did not fit the pending batch was emitted BEFORE it (three 400-byte sends against a 1000-byte cap left as sequence 2, then 0), which the uncompressed batch budget made far more frequent;
- an encrypting, compressing, batching producer emitted keyless batches that the new consumer dropped entry by entry.

## Decision

1. **The codecs move to `magnetar_proto::compress`**, with their decompression-bomb guards unchanged.
   `magnetar_runtime_tokio::compress` stays a public re-export, so no public path breaks.
   A new `decompress_within(kind, bytes, limit)` decodes a payload whose exact size the wire does not carry: it drops `decompress`'s exact-size check and keeps the ceiling — Snappy's announced length and LZ4's output buffer are bounded before any allocation, and the two streaming codecs stop one byte past the limit.
2. **`ConsumerState::deliver` resolves a compressed batched entry before splitting it**, trying two layouts in a fixed order:
   1. **Java layout** — `decompress` the whole body to EXACTLY `uncompressed_size`, then split it into all `num_messages_in_batch` members.
      The exact-size check is what tells this layout from the next one, so it is never relaxed.
   2. **Legacy layout** — split the raw body into all `num_messages_in_batch` members, then decode each with `decompress_within` under one `MAX_FRAME_SIZE` budget shared by the whole entry.
      Members are committed only when a layout decodes in full, so a Java attempt that fails halfway never surfaces a member twice.
      Only when the legacy layout fails as well does a Java body that decoded to exactly `uncompressed_size` but split short surface the members before the malformed one, as Java's `receiveIndividualMessagesFromBatch` does; the rest are refunded as below.
      A decoded legacy member is shrunk to its decoded length before it is queued, so the receiver queue holds only plaintext bytes.
      Every surfaced member carries `compression = None`, which is the marker that keeps both engines' post-pop decompression off it; the producer's `uncompressed_size` stamp is left as it arrived.
      An uncompressed batch is split as before and keeps the members that parsed before a malformed one; an unbatched compressed message is untouched and still decompressed after the pop.
      An entry is batched whenever it carries `num_messages_in_batch`, `Some(1)` included: Java takes the single-message path only when the field is absent (`numMessages == 1 && !hasNumMessagesInBatch()` in `ConsumerImpl.messageReceived`), and a one-member batch still carries the member framing.
3. **Defensive accounting.** Every position the broker charged — unacked in the delivered ADR-0105 `ack_set` — that no layout surfaced is debited (`record_dispatch_unit`) AND refunded (`record_broker_permit_consumed`) at delivery, and the entry logs ONE structured `warn!` (`consumer_id`, `ledger_id`, `entry_id`, `num_messages_in_batch`, `decoded`, `undecoded`, `compression`).
   That covers both layouts failing, an unknown codec, an `uncompressed_size` above the frame ceiling, and a guard firing mid-batch.
   `conn.rs` already calls `maybe_flow` after every `deliver`, so the refund alone re-arms flow.
   This is a fifth refund site under ADR-0107's rule — a unit is refunded the moment the client decides it will never be popped.
   **Deviation from Java.** `ConsumerImpl.discardCorruptedMessage` refunds ONE permit (`increaseAvailablePermits(cnx)`) for an entry the broker charged `numMessagesInBatch` permits for; magnetar refunds every charged position, so its mirror and the broker's balance stay in step.
   The level is `warn!`, not the `error!` ADR-0054 gives a corruption drop no caller sees: the consumer recovers on its own (ADR-0054's "degraded but recovering"), and the volume is bounded by the number of undecodable entries, never by healthy throughput.
4. **`ProducerState` compresses.**
   A batched message stays uncompressed until `flush_batch` compresses the whole concatenation once and stamps `uncompressed_size` with its pre-compression length, as `BatchMessageContainerImpl` does; the batch byte budget therefore counts uncompressed bytes, as Java's container does.
   A message that cannot join the pending batch flushes it first, so it never overtakes the batch on the wire: a batchable one then starts the next batch (Java `ProducerImpl.doBatchSendAndAdd`), and one too large for even an empty batch is sent on its own right behind it.
   An unbatched message is compressed in `queue_send` before the chunking decision, so the chunk count comes from the compressed size (`ProducerImpl.sendAsync` / `serializeAndSendMessage`); chunking still compresses before it chunks.
   The tokio engine no longer compresses, with one exception: an ENCRYPTING producer compresses before it encrypts, because PIP-4 puts the compressed bytes inside the envelope (`ProducerImpl.java:986-1003`), and names the codec on the metadata itself.
   A payload whose metadata carries `encryption_keys` or names a codec is **already encoded**: the state machine neither compresses it again — compressing ciphertext would put the codec outside the envelope — nor batches it, because a batch drops the members' keys and would compress codec output a second time.
   So an encrypting producer sends every message unbatched even with batching enabled, until the batched + encrypted follow-up lands; that path round-trips end to end.
   The tokio engine's memory reservation is taken before compression and so counts uncompressed bytes.
5. **The moonpool engine is unchanged at its edges.**
   Its producer still refuses every codec; its consumer now reads compressed batched entries through the shared `deliver`, but still has no post-pop decompression for an unbatched compressed message.

## Consequences

- magnetar ↔ Java interoperates on compressed batches in both directions, verified against real `pulsar-perf` and `pulsar-client` inside the broker container (`crates/magnetar/tests/e2e_compressed_batch_interop.rs`).
- **The wire layout of a compressed batch magnetar produces changes** to the Java layout.
  magnetar still reads the legacy layout — and, for the first time, actually reads it — so legacy backlogs drain.
  **A magnetar consumer at 1.7.2 or older WEDGES on the new layout**, exactly as it wedged on Java's: it queues nothing, never refunds the batch's permits, stops sending `CommandFlow` and logs nothing — the issue #860 symptom.
  In a mixed fleet, upgrade every consumer before any producer.
  One legacy case stays unreadable: an entry whose members together inflate past `MAX_FRAME_SIZE` — the default 128 KiB batch budget of compressed members at a ratio above 40×, a shape highly repetitive logs can take — exhausts the shared budget, and is dropped and refunded with the `warn!` above rather than read.
- An undecodable entry no longer wedges the consumer, but it is dropped without an ack: its positions stay outstanding at the broker, so a durable subscription's mark-delete position stays behind it until something else acknowledges it.
- `magnetar-proto` now links two C libraries, `zstd-sys` and, through `flate2`'s `zlib` feature, `libz-sys`; the crate is no longer pure Rust.
- `deliver` now decompresses a batched entry — up to `MAX_FRAME_SIZE`, 5 MiB, per entry — while the runtime holds the connection mutex and the consumer slot lock (`Connection::handle_bytes`, `conn.rs`'s `Message` arm), so a large compressed batch lengthens that critical section for every handle on the connection.
- An encrypting producer with batching enabled now sends unbatched, one entry per message: more entries and more `CommandSend` frames for the same throughput, in exchange for entries a consumer can decrypt.
- Codec work moves into the state machine's critical sections.
  On the send path it runs under the per-producer slot lock only, serialising compression per producer; a flush driven by `batching_max_publish_delay` or an explicit `flush` reaches `flush_batch` through `Connection`, so that batch is compressed under the connection lock too, on the driver's path.
  The lock order of ADR-0038 is unchanged.
- Single-message test fixtures across the workspace stamped `num_messages_in_batch = Some(1)` on raw payloads, a shape Java reads as a corrupt batch; they now omit the field.
- The `magnetar_runtime_tokio::compress` unit tests moved with the code into `magnetar-proto`; the runtime parity count is rebalanced by tokio-only tests of the tokio-only paths (the encrypting send, the post-pop decompression, the re-exported path) and by deleting the two moonpool placeholders that stood for the moved tests and claimed moonpool had no decompression path at all.

### Follow-ups

- **Batched + encrypted.** Producer side: Java encrypts the whole compressed batch body once and stamps the keys on the batch metadata; magnetar sends encrypted messages unbatched until `flush_batch` can do the same.
  Consumer side: a Java batched + encrypted entry has to be decrypted as one body, then decompressed, then split; `deliver` cannot decrypt (the decryptor lives in the engine, after the pop), so such an entry is undecodable today and is dropped and refunded with the `warn!` above.
- **Reader start position inside a batch.** The Reader has no start-message-id batch-index skip, so reopening at a position inside a batch re-delivers members `0..k-1`; Java skips and refunds them (`ConsumerImpl.java` ~`:1318-1324` and ~`:1874-1875`).
- **Negative broker overshoot.** `permit_balance` is a `u32` decremented with `saturating_sub`, so it cannot represent the negative balance a forced whole-entry dispatch gives the broker.
- **`receive_batch` inner pops** do not notify the driver, so a flow they queue waits for the next driver wake.
- **Moonpool codecs.** The moonpool producer still refuses every codec, and its consumer still cannot decompress an unbatched compressed message.

## References

- `crates/magnetar-proto/src/compress.rs` — the codecs, `decompress_within`.
- `crates/magnetar-proto/src/consumer.rs` — `ConsumerState::deliver`, `unpack_batch_members`, `legacy_batch_members`, `split_batch_body`.
- `crates/magnetar-proto/src/producer.rs` — `ProducerState::queue_send` / `compress_unbatched` / `flush_batch`, `already_encoded`.
- `crates/magnetar-runtime-tokio/src/producer.rs` — `Producer::send`, compression only for an encrypting producer.
- Tests, every layer: the `consumer::tests` / `producer::tests` blocks marked issue #860 in `magnetar-proto`; `crates/magnetar-runtime-{tokio,moonpool}/tests/compressed_batch_flow.rs`; `crates/magnetar-differential/tests/compressed_batch_flow_equivalence.rs`; `crates/magnetar/tests/e2e_compressed_batch_interop.rs`; `e2e_crypto_with_compression_and_batching` in `crates/magnetar/tests/e2e_crypto.rs`.
- Apache Pulsar Java client (external): `ProducerImpl.doBatchSendAndAdd`, `BatchMessageContainerImpl.getCompressedBatchMetadataAndPayload`, `ConsumerImpl.uncompressPayloadIfNeeded`, `ConsumerImpl.receiveIndividualMessagesFromBatch`, `ConsumerImpl.discardCorruptedMessage`, `ProducerImpl.sendAsync`, `ProducerImpl.serializeAndSendMessage`.
- [ADR-0044](0044-moonpool-message-crypto-bridge.md), [ADR-0054](0054-logging-policy.md), [ADR-0105](0105-read-the-delivered-batch-index-ack-set.md), [ADR-0107](0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md).
