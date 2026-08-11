# ADR-0096 — Reconnect batch and durable-cursor safety

- **Status**: Accepted
- **Date**: 2026-08-06
- **Decider**: Florentin Dubois
- **Tags**: reconnect, batching, acknowledgements, pip-54, durable-subscription
- **Amends**: [ADR-0055](0055-bit-flip-survivability-model.md) (durable reconnect cursor only; its terminal-failure and bit-flip decisions stand)

## Context

Three reconnect paths trusted state that a dead transport could no longer prove.

Producer batching creates one `OpSend` per logical message, but `flush_batch` emits one ranged wire frame and never stores that shared frame in any per-message `replay_frames` field.
`Connection::reset` drained those non-replayable operations, woke their futures without an outcome, and left them outside both replay and timeout tracking, so every batched send cut before its receipt could remain pending forever.

PIP-54 tracks the unacknowledged positions of each producer-batched broker entry in `ConsumerState::batch_ack_tracker`.
Reset correctly clears that session-local tracker because an ack sent before disconnect may not have reached the broker, but a later individual ack whose tracker was absent fell through as a full-entry ack with no `ack_set`.
With broker-side `acknowledgmentAtBatchIndexLevelEnabled=true`, that could delete siblings the application had not acknowledged.

Finally, every consumer reattach copied the highest locally submitted ack into `CommandSubscribe.start_message_id`.
A submitted ack is neither confirmed nor a contiguous frontier under Shared and Key_Shared ordering, while an established durable subscription already has a broker-persisted cursor.
Apache Pulsar's Java client omits the start id for a durable reattach and lets that cursor govern recovery.

ADR-0055's simulation broker described reconnect by setting `start_message_id = last_acked_message_id`.
That was a test-model shortcut, not a safe client cursor contract, and this ADR amends it.

## Decision

### Non-replayable batched sends fail deterministically

`ProducerState::snapshot_pending_sends` partitions drained operations into replayable snapshots and non-replayable sends.
Single and chunked sends retain transparent at-least-once replay through their cached frames.
Every batched operation has no independent replay frame, before or after `flush_batch`, so reset installs `OpOutcome::SendError { code: -1, message: "batched send cannot be replayed after connection reset" }` before waking its future.

The client does not copy one ranged batch frame into every logical `OpSend`: doing so would publish the full batch once per member.
It also does not retain a second raw-message representation solely for reconnect; callers receive a bounded unknown-outcome error and may retry according to their delivery semantics.

### Missing PIP-54 state is reconstructed conservatively

Reset continues to clear `batch_ack_tracker`.
When a valid batched `MessageId` has no tracker entry, `Connection::ack` creates `BatchAckEntry::fresh(batch_size)`, clears only the supplied `batch_index`, and emits the resulting non-empty `ack_set` until every position has been explicitly acknowledged in the reconstructed state.
Pulsar intersects that set with its persisted cursor set, so positions confirmed before reset stay confirmed while an unconfirmed local bit is never trusted.

Invalid batch coordinates fail locally without emitting `CommandAck`.
The distinct receive-side gap where Magnetar does not yet apply an incoming `CommandMessage.ack_set` remains out of scope.

### Established durable subscriptions use the broker cursor

Every reattachment path uses one rule:

- a durable consumer that has attached successfully before sends no `start_message_id`;
- a never-attached consumer retains the caller's explicit initial start position;
- a non-durable consumer may retain its client-side `last_acked_message_id` fallback.

The rule applies to full reconnect rebuild, transient subscribe retry, and same-broker `CommandCloseConsumer` reattachment.
Seek retains its explicit target-owned resubscribe path.

## Consequences

**Easier.** No batched `SendFut` can be orphaned by reset, an individual stale batch ack cannot become a full-entry ack merely because reset cleared local state, and durable reconnect cannot skip work based on an unconfirmed local maximum.

**Harder.** A post-flush reset reports an unknown publish outcome; retry can duplicate a batch that the broker accepted before the receipt was lost, which is consistent with at-least-once delivery and safer than a silent stall.
Durable reconnect can redeliver according to the broker cursor.

**Compatibility.** ADR-0055's `start_message_id = last_acked_message_id` simulation statement is superseded by the broker-authoritative durable rule here; its terminal-failure and bit-flip decisions remain binding.
No public API, dependency, channel, I/O in `magnetar-proto`, or new clock source is introduced.

**Test contract.** The ADR-0024 layers cover reset before batch flush, after flush before receipt, and after receipt; conservative PIP-54 reconstruction with `acknowledgmentAtBatchIndexLevelEnabled=true`; durable Shared and Key_Shared reattach without a local start id plus fresh and non-durable controls; Tokio/Moonpool parity and differential projections; and a frame-aware real-Pulsar gate that cuts the transport and inspects reattach and ack frames.

## References

- Issues [#395](https://github.com/CleverCloud/magnetar/issues/395), [#396](https://github.com/CleverCloud/magnetar/issues/396), and [#398](https://github.com/CleverCloud/magnetar/issues/398).
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — mandatory cross-runtime test layers.
- [ADR-0038](0038-split-connection-mutex.md) — reconnect replay and lock ordering.
- [ADR-0055](0055-bit-flip-survivability-model.md) — terminal failure and the superseded local-cursor simulation shortcut.
