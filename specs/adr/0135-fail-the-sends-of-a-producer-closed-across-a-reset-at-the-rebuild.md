# ADR-0135 — Fail the sends of a producer closed across a reset at the rebuild

- **Status**: Accepted
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: producer, close, reconnect, send, state-machine, tokio, moonpool, sans-io
- **Amends**: [ADR-0111](0111-share-one-memory-limit-controller-per-client.md)

## Context

`Producer::close()` and the engines' last-clone drop guard mark the producer slot closed and register a `CommandCloseProducer` request.
They fail nothing: the producer's unacknowledged sends stay in its `pending` queue until the broker acknowledges the close, whose `Success` arm runs `Connection::fail_sends_of_closed_producer` (ADR-0111, issue #867) and resolves each send with `OpOutcome::Terminal`.
The `SendFut` holds only the shared connection state and the producer handle, so it outlives `Producer::close(self)` and the drop guard.

That acknowledgement is the only thing that resolves those sends, and a reconnect can make it impossible.

- **A close unacknowledged when the session resets.**
  `Connection::reset()` takes `pending_requests` wholesale, so the close request is gone and its acknowledgement can never arrive.
  `reset()` therefore runs `fail_sends_of_closed_producer` for every close request it takes, before it snapshots the replayable sends, and this case resolves already.
- **A close issued between the reset and the rebuild.**
  `reset()` has already moved the producer's replayable sends into `in_flight_publish_snapshots` and woken each future without an outcome, relying on the replay to resolve them.
  `rebuild_producers()`, which the driver runs right after `CONNECTED`, re-attaches only open producers and then discarded the snapshot of every other handle with `retain`, so when the close acknowledgement arrives the bucket is gone and `fail_sends_of_closed_producer` finds nothing to drain.
  The discarded `OpSend` was in no queue, no snapshot bucket and no pending request: no outcome, no waker and no `send_timeout` sweep (which walks live queues and existing buckets) could reach its future, and it stayed pending until `fail_all_pending`, at client shutdown or supervisor give-up.

The comment on the discard said the user-facing close path would surface the disposition; for a close issued after the reset, that path runs only once the rebuild has discarded the bucket.
ADR-0111 lists the discard as a release point for the sends' memory reservation, which is intended; it states no outcome for the future, and its Consequences promise that a send pending at its producer's close resolves with `OpOutcome::Terminal` instead of hanging forever.

## Decision

`rebuild_producers()` fails the snapshot bucket of every handle it does not re-attach, instead of dropping it.

- Each `OpSend` of such a bucket resolves through `terminalize_snapshot_bucket` with `OpOutcome::Terminal` and the reason `producer closed before the send was acknowledged` (`PRODUCER_CLOSED_REASON`, the one the close acknowledgement uses), is woken through its own waker or the connection slab, and is dropped, which releases its memory reservation exactly as the discard did.
  The tokio and moonpool `SendFut` translate it to `ClientError::PeerClosed`.
- The handles are those with a bucket that are not among the producers `rebuild_producers` just re-attached, that is the closed ones.
- A close acknowledgement that arrives later runs `fail_sends_of_closed_producer`, whose bucket drain is a no-op for a handle with no bucket, and whose `pending` drain is empty: nothing is resolved or released twice.
- `reset()` is unchanged.
  It already fails the sends of the close requests it takes; failing more there would not cover a close issued after the reset, and the snapshot sweep of `handle_timeout` still bounds the relocated sends by `send_timeout` while the new session handshakes.
- No new public API, no wire change.

## Consequences

- A send in flight at a producer close resolves, with a failure, whatever the transport does between the close and its acknowledgement, and its memory reservation is released at the same point as before.
- A closed producer's sends fail when the rebuild runs, so after the new session's handshake rather than at the reset.
  Until then they are bounded by `send_timeout` through the snapshot sweep, which runs in the driver loop of the new session: the bucket exists only once the new socket is dialled, and nothing sweeps during the supervisor's cooldown, backoff or dial, where `fail_all_pending` at give-up is the bound.
- `Producer::close()` still returns `ClientError::Other` when the reset severs its own request: `wait_request` maps `OpOutcome::SessionLost` to an unexpected-outcome error in both engines.
  That is a different contract (what a close that raced a reconnect should report) and is not changed here.
- Cost: one pass over the snapshot map keys per rebuild.
- Incompatible with: a caller that relied on a closed producer's relocated sends staying pending across a reconnect, which the hang made indistinguishable from a leak.

### ADR-0024 test layers

(a) `magnetar-proto/tests/producer_close.rs` (`send_in_flight_at_awaited_close_resolves_when_the_session_resets_before_the_close_ack`, the forgotten-close twin and `send_relocated_by_reset_resolves_when_the_producer_is_closed_before_the_rebuild`); (b) `magnetar-runtime-tokio/tests/producer_close_inflight_reset.rs`; (c) `magnetar-runtime-moonpool/tests/producer_close_inflight_reset.rs`; (d) `magnetar-differential/tests/producer_close_inflight_reset_equivalence.rs`.
In each file the first two cases (a close still unacknowledged at the reset) read the send's outcome right after `reset()`, before the rebuild could resolve it, so they pin the `reset()` path that already resolves those sends and pass without this decision; the third (a close between the reset and the rebuild) is the case it fixes.
End-to-end: in `crates/magnetar/tests/e2e_producer_close_across_reconnect.rs` a TCP proxy in front of a real broker drops one publish, cuts the session and holds the broker's `CommandConnected` on the redial while the producer is closed; the relocated send must resolve.

## References

- `crates/magnetar-proto/src/conn.rs` — `Connection::rebuild_producers`, `terminalize_snapshot_bucket`, `fail_sends_of_closed_producer`, `reset`.
- [ADR-0111](0111-share-one-memory-limit-controller-per-client.md) — the reservation release points and the promise that a send pending at close resolves; its list entry "a snapshot bucket `rebuild_producers` discards for a closed producer" now reads "fails".
- [ADR-0059](0059-terminal-fast-fail-new-ops.md) — `fail_all_pending`, the only other resolver of a stranded send.
