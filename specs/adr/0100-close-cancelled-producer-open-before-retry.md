# ADR-0100 — Close a cancelled producer open before retrying it

- **Status**: Accepted
- **Date**: 2026-08-21
- **Decider**: Florentin Dubois
- **Tags**: producer, cancellation, resilience, sans-io, retry

## Context

Issue #406: an `open_producer` lost client-side while completing broker-side leaks a zombie broker-side producer session.
Every later open under the same producer name is rejected with `ProducerBusy` (code 16, the broker's `NamingException` "Producer with name 'X' is already connected to topic 'T'") for as long as the connection lives.
Only `pulsar-admin topics unload` recovered the name.

The path is entirely inside cancellation:

- `Connection::create_producer` (`crates/magnetar-proto/src/conn.rs`) allocates a FRESH producer id per call — every engine retry included — and `emit_command_producer` writes the `CommandProducer` into the connection's outbound buffer and registers `pending_requests[request_id] = ProducerOpen { handle }`.
- The engines' open path (`Client::open_producer_with_operation_deadline` in `crates/magnetar-runtime-tokio/src/client.rs`, mirrored in `crates/magnetar-runtime-moonpool/src/producer.rs`) races that open against the ADR-0080 operation deadline. On expiry the armed `PendingProducerOpenGuard` drops and calls `Connection::cancel_producer_open`.
- `cancel_producer_open` was local-only. Its doc comment stated the premise outright — "the broker has not acknowledged the handle, so no `CommandCloseProducer` is required" — and it removed the pending request, the producer slot, and the replay request without emitting anything.

That premise is false. The broker never learns the client gave up: it completes the open on its own schedule and keeps the `(topic, producer_name)` registration.
A late `CommandProducerSuccess` then finds no pending entry and is dropped silently, so the registration the client just heard about is never reaped.

`ProducerBusy` is retryable for `OperationKind::ProducerOpen` (`crates/magnetar-proto/src/operation_retry.rs`, ADR-0080), so the engines' retry loop re-issues the open with a fresh producer id against the same zombie until the retry budget or the operation deadline runs out.
Every attempt strands one more registration.

The same shape reaches production a second way, with no client-side deadline involved at all: a client process dies with a lingering proxy-mediated connection, and nothing is left to send any close.

ADR-0057 does not cover this. Its last-clone drop guard only exists once an open has SUCCEEDED and a `Producer` handle exists; a producer that never finished opening has no handle to drop.

The reusable mechanism already existed: `Connection::close_producer_forget` encodes a `CommandCloseProducer` for a raw producer id without needing a live slot and registers `PendingRequestKind::ProducerCloseForgotten`, whose `Success` / `Error` acks are consumed in place instead of recording an `OpOutcome` nobody drains (issue #241).

### Alternatives considered

- **Reuse the producer id across retries.** Would collapse N zombies into one, not zero, and would collide with the broker's own epoch/fencing handling of a re-attach.
- **Make `ProducerBusy` terminal for `ProducerOpen`.** Turns a recoverable transient (a genuinely busy name during a legitimate failover) into a hard failure, and still leaves the leaked registration behind.
- **Always suffix producer names.** Breaks every behaviour keyed on producer identity — `Exclusive` / `WaitForExclusive` fencing, broker-side sequence-id dedup across a restart, dashboard continuity. Kept, but as an opt-in (below).
- **Randomise the name inside `magnetar-proto`.** Refused: `magnetar-proto` takes no non-determinism it was not handed (ADR-0011; the two documented leaks are inventoried in `ARCHITECTURE.md` and this would be a third).

## Decision

### Cancellation emits a best-effort close when the broker may hold a registration

`Connection::cancel_producer_open` keeps every local purge it already did and additionally calls `Connection::close_producer_forget(handle)` when the broker may still hold a registration for that producer id.
That condition is exactly:

- a `ProducerOpen` request for the handle is still pending, **or**
- the producer is already `broker_ready`,

and the slot is not already `closed`.

Both disjuncts are session-scoped by construction.
`Connection::reset` takes `pending_requests` wholesale and is the only place that clears `outbound`, and it clears every producer's `broker_ready` through `ProducerState::snapshot_pending_sends`.
So a pending entry proves the `CommandProducer` is buffered or already flushed on the CURRENT session, and a session lost to `reset` satisfies neither disjunct — correctly, because a broker reaps every producer of a dead connection and a close naming that producer id on the rebuilt session would name an id the new session never registered.
A second cancellation finds no slot and emits nothing, so cancellation stays idempotent.

Pulsar processes commands in order per connection, so the `CommandCloseProducer` written after the `CommandProducer` on the same stream reaps the registration whether or not the broker had already completed the open.
An unknown-producer rejection is consumed in place by the `ProducerCloseForgotten` handlers and surfaced as a `warn!` — never as an undrainable `OpOutcome`.

### The late `ProducerSuccess` path is unchanged, and that is sufficient

A `CommandProducerSuccess` whose request id has no pending entry is still dropped: no outcome, no waker, no slot resurrection.
No second close is emitted there, and none is needed.
By the ordering argument above, the registration that success announces is already reaped by the close cancellation wrote onto the same connection before the success could be read.
The only way the client sees such a success without having written that close is a session that died instead — and its bytes never reach the decoder.
Adding a late-success close would need a cancelled-request → handle map that outlives the request, i.e. exactly the unbounded-state leak the forgotten-close mechanism exists to avoid.

### Opt-in unique producer-name suffix on the façade

`ProducerBuilder::unique_name_suffix(bool)` appends an engine-generated suffix to the name set by `ProducerBuilder::name`.
It is **off by default** — a pinned name stays pinned — and is a no-op with no name set, since the broker already assigns a unique one.

The suffix comes from the existing `Engine::random_subscription_suffix` seam that `ReaderBuilder` already uses: `uuid::Uuid::new_v4().simple()` on tokio, a process-global counter on moonpool so simulation names stay reproducible.
It lives in the façade, not in `magnetar-proto`.

This is the answer to the variant the close cannot reach: a registration stranded by a dead client, or behind a proxy-mediated connection that outlived it.
Its cost is the reason it is opt-in — a unique name breaks access-mode fencing, cross-restart sequence-id dedup, and per-name dashboards and metrics.

## Consequences

- A timed-out or otherwise cancelled producer open no longer poisons its name for the life of the connection. The immediately following retry sees a free name, so the ADR-0080 retry budget is spent on real transients instead of on a self-inflicted `ProducerBusy`.
- One extra `CommandCloseProducer` per cancelled open. It is fire-and-forget, allocates one request id, and its ack is consumed in place, so it adds no waiter, no outcome, and no per-cancel memory.
- A cancellation that races a broker rejection emits nothing: the `Error` handler removed the pending entry before the guard ran, and a rejected open holds no registration.
- Callers that pin a producer name and expect `Exclusive` fencing are unaffected — the suffix is opt-in and the close only ever removes a registration this client created.
- ADR-0080's cancellation clause is amended (below), not replaced: cancellation still owns all local correlation state, is still idempotent, and still ignores late broker replies.
- `check-sim-coverage`: the new proto lines are reached from the moonpool and differential test binaries (`crates/magnetar-runtime-moonpool/tests/producer_open_cancel_close.rs`, `crates/magnetar-differential/tests/producer_open_cancel_close_equivalence.rs`), which the gate executes.

### Amends ADR-0080

[ADR-0080](0080-configurable-operation-retry-policy.md) § "Producer and consumer attachment lifecycle" states:

> Canceling a setup request removes its correlation, landed outcome, and queued success, failure, or broker-close attachment event, and late broker replies for that request are ignored.

and § "Cancellation owns all local correlation state" states:

> Dropping or timing out an opening producer or consumer removes its pending request and handle state.
> Cancellation is idempotent; late broker replies are ignored.

Both remain true of the LOCAL state, and late replies are still ignored.
What is amended is the implied completeness: cancelling an opening PRODUCER is no longer local-only.
It also writes one best-effort `CommandCloseProducer` under the conditions above, because local-only cancellation is what stranded the broker-side registration.
Cancelling an opening consumer is unchanged — `CommandSubscribe` carries the subscription name the broker keys on, and a subscribe that never completed leaves nothing to reap.
Every other ADR-0080 decision — the retry policy, the busy classification, the single operation deadline, the per-generation request ownership — remains binding.

## References

- `crates/magnetar-proto/src/conn.rs` — `cancel_producer_open`, `close_producer_forget`, and the `ProducerSuccess` / `Success` / `Error` handlers that consume a forgotten close.
- `crates/magnetar/src/builders.rs` — `ProducerBuilder::unique_name_suffix`.
- `crates/magnetar/src/engine/mod.rs` — the `Engine::random_subscription_suffix` seam.
- `crates/magnetar-runtime-tokio/tests/producer_open_cancel_close.rs`, `crates/magnetar-runtime-moonpool/tests/producer_open_cancel_close.rs`, `crates/magnetar-differential/tests/producer_open_cancel_close_equivalence.rs`, `crates/magnetar/tests/e2e_producer_open_cancel.rs` — ADR-0024 layers (b), (c), (d) and the e2e layer.
- [ADR-0080](0080-configurable-operation-retry-policy.md) — the retry policy and the cancellation clause amended here.
- [ADR-0057](0057-producer-last-clone-drop-close.md) — the last-clone drop guard, which covers the post-open half of the same leak (issues #241 / #243).
- [ADR-0011](0011-clock-injection-sans-io.md) — why the name suffix is generated in the façade rather than in `magnetar-proto`.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the four-layer test policy this change lands under.
