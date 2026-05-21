# Codex cross-check on the quasar plan

Source: `codex exec` (codex-cli 0.132.0), run 2026-05-20. Codex read `/home/florentin/.claude/plans/ask-quasar-plan.md`, `/home/florentin/.claude/plans/ask-quasar-research.md`, `/home/florentin/.claude/plans/ask-quasar-audit.md`, and grep'd through `/home/florentin/Sources/github.com/apache/pulsar`.

## Q1 — Sans-io decomposition shape

**Verdict: keep the flat `Connection` event-bus + handle-based façade.** Pulsar producer/consumer ids are not independent byte streams like HTTP/2 — they are command-correlated actors over one broker connection with cross-cutting reconnect, lookup, flow permits, batching, dedup, schema, ping, and close behaviour. The plan already has the right hybrid: `handle_bytes` / `poll_transmit` / `poll_event` / timers + `open_producer` / `subscribe` / `send(h, …)` handles. Treat h2 as a *flow-control reference only*. **Reject** a public `SendStream`/`RecvStream` core; the stream metaphor hides connection-scoped behaviour Pulsar does not honour.

## Q2 — moonpool adapter point

**Confirm with refinement.** Skipping `moonpool-transport` is correct (its CRC32C length-prefixed framing conflicts with Pulsar — research §6). But the audit's "NetworkProvider + TimeProvider only" framing is too narrow:

- **Needed**: `NetworkProvider`, `TimeProvider`, **`TaskProvider`** (to spawn the connection actor — already implicit in the plan's `MoonpoolEngine<N,T,R>`), **`RandomProvider`** (deterministic producer names, request ids, backoff jitter, test seeds).
- **Not needed**: `StorageProvider` (no durable local cursors/cache in v0.1.0).
- **Keep**: `moonpool-core` strictly behind the moonpool-runtime crate; *never* leaks into `quasar-pulsar-proto` (research §6 — moonpool is "hobby-grade" + async-trait risk).

→ **Plan amendment**: add `TaskProvider` and `RandomProvider` to the documented adapter surface in §6 of the plan.

## Q3 — Chunking + batching + dedup + encryption order

**The plan's mental model is wrong on one critical point: chunks are never batched; batches are never chunked.**

The Java pipeline (`ProducerImpl.java`):

1. Non-batch path: compression first (`:581-608`).
2. Populate schema + metadata excluding sequence id (`:621-628`).
3. Decide `totalChunks`. **If `canAddToBatch(msg)`, force `totalChunks = 1`** (`:630-654`) — i.e. *batchable → no chunking*.
4. Chunk loop assigns/reuses `sequenceId` (`:696-704`, `:745-753`).
5. **Chunked path**: slice the already-compressed payload → set per-chunk metadata → encrypt each chunk → send (`:775-790`, `:831-844`, `:986-1003`).
6. **Batched path** (only when `totalChunks <= 1`, `:793-818`): batch container serialises singles → compress the whole batch → encrypt → set batch metadata → send (`BatchMessageContainerImpl.java:172-179`, `:267-327`).

→ **Plan amendment**: rewrite §4 step 3 (Producer state machine) to reflect this mutual exclusion. The Rust state machine has two distinct emit paths (`emit_chunked` and `emit_batched`), not one unified "chunk + batch" path. Add a unit test that asserts `canAddToBatch ⇒ totalChunks == 1`.

## Q4 — Schema-registry version negotiation

**Not universally clean — there is an interop trap for `PROTOBUF_NATIVE` and `KeyValue`.**

`GET_OR_CREATE_SCHEMA` flow:
- `ServerCnx.java:3289-3305` → `tryAddSchema` → `ServerCnx.java:3790-3794` → `topic.addSchema` → `AbstractTopic.java:747-752` → `schemaRegistryService.putSchemaIfAbsent`.
- Version lookup (`SchemaRegistryServiceImpl.java:405-418`, `:657-662`): **AVRO / JSON / PROTOBUF** are canonicalised by parsing with Avro `Schema.Parser` and comparing parsed schema equality.
- **All other types, including `PROTOBUF_NATIVE`, hash raw `schema.getData()` bytes** (`SchemaRegistryServiceImpl.java:429-438`).
- Validators (`SchemaDataValidator.java:61-63`, `ProtobufNativeSchemaDataValidator.java:29-39`) deserialise `PROTOBUF_NATIVE` but do NOT canonicalise before storage / compare.
- Stored schema persists raw client bytes (`SchemaRegistryServiceImpl.java:209-220`).

**Consequence for quasar**: Rust must emit Java-compatible canonical bytes for `PROTOBUF_NATIVE` descriptors (and KeyValue nested schema bytes) or the broker creates a *new* schema version on every Rust producer/consumer attach.

→ **Plan amendment**: §7 (Schema layer) must add a "byte-for-byte canonicalisation parity" subsection for `PROTOBUF_NATIVE` and `KeyValue` (even though both are deferred from v0.1.0 scope, this constrains the v0.1.0 schema-registry round-trip).

## Q5 — cargo-mutants timing

**Disagree with audit. Move a small, gated `mutants-smoke` job into v0.1.0** — not v0.2.0.

- The sans-io state machine (`quasar-pulsar-proto`) is the fastest, highest-risk layer; pure code is exactly where mutation testing pays off.
- *Don't* full-workspace `cargo-mutants` in normal CI. Scope a `mutants-smoke` job to `quasar-pulsar-proto` only; time-boxed; nightly/manual.
- Focus mutants: frame decode, request correlation, resend/dedup, flow permits, chunk metadata, timeout transitions.
- Waiting until after v0.1.0 lets the public sans-io shape fossilise before mutation tests catch design holes.

→ **Plan amendment**: §10 (Test strategy) — add `mutants-smoke` as M5/M6 deliverable, not post-v0.1.0.

## Codex biggest-doubts list

1. **Schema bytes parity** — `PROTOBUF_NATIVE` and `KeyValue` nested schemas are the most likely interop trap (`SchemaRegistryServiceImpl.java:429-438`). Even if these are out-of-scope for v0.1.0, the schema-registry GetOrCreate flow must emit deterministic canonical bytes from day one.

2. **Producer resend / dedup around batching+chunking** is more subtle than the plan suggests; sequence id, highest-sequence id, and callback completion differ by path (`ProducerImpl.java:793-868`, `BatchMessageContainerImpl.java:317-327`). Worth its own state diagram in `ARCHITECTURE.md`.

3. **TLS / auth under moonpool** is still under-specified — the plan defaults to TLS-less moonpool engine for v0.1.0 (`ask-quasar-plan.md:522-528`). Codex recommends elevating moonpool TLS to a Tier-1 user question rather than an implementer-deferred item.
