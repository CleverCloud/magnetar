# ADR-0128 — Configure a bounded decompressed-payload ceiling per consumer

- **Status**: Accepted (supersedes [ADR-0112](0112-compress-and-decompress-a-batch-as-one-body.md)'s fixed 5 MiB batch output ceiling)
- **Date**: 2026-11-19
- **Decider**: Florentin Dubois
- **Tags**: compression, consumer, memory, issue-912

## Context

The wire frame remains capped at 5 MiB, while a valid compressed payload can expand beyond that limit.
The previous decompressor also used 5 MiB as its output ceiling, so consumers could not opt in to larger payloads.
Legacy batch entries need one shared output budget across all members, and the aggregate scalable receiver retains its separate `ReceiverBudget` for queued messages, metadata, and decode workspace.

## Decision

Add `max_uncompressed_message_size`, in bytes, to the consumer subscribe request and expose it through ordinary, reader, typed, multi-topic, partitioned, pattern, V5, and scalable consumer builders.
The default remains 5 MiB.
Zero is accepted as a configured ceiling; values above `isize::MAX - 1` cannot back a Rust decompression allocation and are rejected before allocation when used.
This setting limits decompressed output, not the encoded wire frame.
Messages stamped with compression `None` are not decompressed and remain bounded by the unchanged wire-frame limit.

Java-layout compressed batches decompress one body against this per-consumer ceiling before splitting.
Legacy-layout batches debit the actual decoded output from one whole-entry balance initialized to the same ceiling; no member is committed unless the entry decodes fully, and failed members retain the existing FLOW refund behavior.
Legacy zstd stream frames use a 2 MiB minimum window bound because Pulsar's level-3 producer default advertises that window even for smaller payloads; larger configured output ceilings raise this bound, and output remains bounded by the remaining per-entry budget.
The exact-size bulk zstd path keeps a window bound derived from its declared output size.

Scalable children inherit the configured per-consumer ceiling and reject declared or fallback payload lengths before reserving codec workspace.
Their independent `ReceiverBudget` continues to cap retained delivery data and transform workspace; increasing the output ceiling does not enlarge that budget.

## Consequences

Applications can receive compressed payloads above 5 MiB by explicitly increasing the per-consumer limit, up to the platform's allocation bound.
Larger ceilings permit larger temporary decode output and retained messages, so applications should account for consumer count and queue depth when choosing a value.
Direct struct-literal callers of `SubscribeRequest`, `StreamConsumerOptions`, and `SegmentConsumerOptions` must initialize the newly added field; builder and `Default` callers keep their existing source shape.
The five behavioral test layers remain required: proto unit, Tokio integration, Moonpool integration, differential parity, and broker-backed façade e2e.
