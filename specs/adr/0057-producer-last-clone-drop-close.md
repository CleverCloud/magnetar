# ADR-0057 — Producer last-clone drop fires a best-effort fire-and-forget close

- **Status**: Accepted
- **Date**: 2026-06-11
- **Decider**: Florentin Dubois
- **Tags**: producer, lifecycle, raii, runtime-tokio, runtime-moonpool, proto

## Context

Producers had no `Drop` impl on either engine.
Releasing every `Producer` clone without an explicit `close().await` left the broker-side registration alive for as long as the shared TCP connection stayed open — the broker only garbage-collects producers when their connection dies, and all producers of a broker share one connection.
Recreating a producer with the same user-provided name on the same topic then failed forever with `NamingException` (broker error code 16).
This was hit in production by otelgw, whose LRU cache evicts idle producers by dropping them and recreates them on demand with a fixed name (the hostname) — issue #241.

Alternatives considered:

- **Explicit close only (status quo)** — every consumer of the crate must never lose a producer handle without `close().await`; any early return, cancellation, or cache eviction silently bricks `(topic, producer_name)`. Violates Rust RAII expectations.
- **`AsyncDrop`** — not stabilised; nightly-only and no guarantee the async drop glue runs in sync contexts.
- **Awaited close in `Drop` via `block_on`** — deadlocks inside async contexts; rejected outright.
- **Naive `impl Drop for Producer`** — wrong: `Producer` is cheap-clone (`Arc` bumps), so any clone's death would close the producer out from under the surviving clones.

A first implementation enqueued the close through the awaited-path `Connection::close_producer`.
Review caught a leak: that entry point registers a `PendingRequestKind::ProducerClose` whose broker ack is recorded as an `OpOutcome` that only a `RequestFut` (via `take_outcome`) ever removes — and the drop path builds no future, so every dropped producer leaked one permanent `outcomes` entry, unbounded growth under exactly the continuous-eviction workload the change targets.

## Decision

Dropping the **last** clone of a `Producer` fires a best-effort, fire-and-forget `CommandCloseProducer`.

- Each `Producer` carries an `Arc<ProducerCloseGuard>` shared by every clone, wired in a single `assemble()` construction point per engine; the guard's `Drop` therefore runs exactly once, when the last clone goes away.
- The guard calls a dedicated sans-io entry, `Connection::close_producer_forget`: encodes the frame, flips the slot's `closed` flag, registers `PendingRequestKind::ProducerCloseForgotten`, and wakes the driver — it never awaits.
- The `Success`/`Error` handlers consume a `ProducerCloseForgotten` ack **in-place** instead of recording an `OpOutcome`: nothing will ever drain it, so recording would leak one entry per dropped producer. A broker rejection surfaces as a `warn!` with structured fields (ADR-0054) so a close storm the broker starts rejecting stays diagnosable.
- The explicit `close().await` stays the reliable path (awaits the broker ack, reports errors) and keeps recording its outcome for the `RequestFut` to drain.
- Dedup is best-effort: the slot's `closed` flag dedups a preceding completed client-initiated close. Broker-initiated detach intentionally keeps `closed = false` (re-attach on PIP-188 migration / failover), and the guard's check+act is non-atomic against a concurrent `close()` — both residual cases emit one redundant `CloseProducer`, which the broker tolerates.
- ADR-0038 lock order is preserved: the guard probes the per-slot flag, releases it, then takes the global connection mutex (sequential, never nested).

## Consequences

- Cache-eviction / early-return / cancellation patterns no longer brick `(topic, producer_name)`; the otelgw LRU workload recreates same-name producers reliably because the drop-close and the follow-up open ride the same connection in order.
- A new user-visible lifecycle semantic: on `main` before this ADR, dropping a producer was a wire no-op. This goes beyond Java parity — Java producers leak broker-side until disconnect when abandoned without `close()`.
- The fire-and-forget path cannot report success to the caller; failures are observable only via the `warn!` log.
- Two close entry points exist in the proto layer (`close_producer` awaited, `close_producer_forget` fire-and-forget); both share one private `close_producer_inner`.
- Both engines carry a 1:1 mirrored guard; the `ConnectionShared` types are distinct concrete types per engine, so the duplication is deliberate (mirror convention) rather than extracted behind a trait.

## References

- `crates/magnetar-proto/src/conn.rs` — `close_producer_forget`, `PendingRequestKind::ProducerCloseForgotten`, in-place ack consumption in the `Success`/`Error` arms.
- `crates/magnetar-runtime-tokio/src/producer.rs`, `crates/magnetar-runtime-moonpool/src/producer.rs` — `ProducerCloseGuard` + `assemble()`.
- Tests (ADR-0024 four layers + e2e): `crates/magnetar-proto/tests/producer_close.rs`, `crates/magnetar-runtime-{tokio,moonpool}/tests/producer_drop_close.rs`, `crates/magnetar-differential/tests/producer_drop_equivalence.rs` (`Op::DropProducer`), `crates/magnetar/tests/e2e_producer_drop.rs`.
- Related: ADR-0038 (lock ordering), ADR-0054 (structured logging), issue #241, PR #243.
