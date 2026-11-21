# ADR-0146 — Release the withheld flow when a consumer resumes

- **Status**: Accepted
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: consumer, flow-control, pause, resume, state-machine, tokio, moonpool, sans-io

## Context

`pause()` stops the automatic flow refill: `ConsumerState::maybe_flow` returns `None` while `paused` is set, so the permits a receive loop hands back (`consumed_since_flow`) accumulate on the client and the broker stops dispatching once the permits it already holds drain.
A receive loop that drains the whole window while paused therefore leaves the consumer in one exact state: the broker holds zero permits (`permit_balance == 0`) and the client owes it the whole window (`consumed_since_flow == receiver_queue_size`).

`resume()` was the one call that can end that state, and it did not.
The tokio and moonpool `Consumer::resume` stored `paused = false` in the slot and woke the driver, and `Connection::set_paused` was the same bare store.
Nothing ran `maybe_flow` and nothing staged the `CommandFlow`, although the code comment on the tokio `resume` ("it may have a flow to emit now that we're un-paused") named the intent.
`maybe_flow` has two callers, the `CommandMessage` arm of `Connection::handle_frame` and `Connection::pop_message`, and neither is reachable from a drained, resumed consumer:

- No arrival comes, because the broker holds no permit it could dispatch against.
- A `receive()` parked on the empty queue before the resume is woken by arrivals, close, end-of-topic and reset only, so it is never polled again and never reaches `pop_message`.
- The stall watchdog (ADR-0101) skips a consumer with `permit_balance == 0`: `is_stall_candidate` requires `permit_balance > 0`.

The canonical back-pressure pattern runs into it: one task pauses, the receive loop drains the buffer and parks on the next `receive()`, another task resumes.
With a fixed receiver queue nothing can release the flow and the consumer stays parked until the application closes it; with an auto-scaled one only the next adjustment tick could grow the window.

A `receive()` first polled after the resume was not affected: `pop_message` runs `maybe_flow` even on an empty pop and staged the flow, at the cost of waiting for the next driver wake (`docs/follow-ups.md` §26 is the same shape).

## Decision

`Connection::set_paused` now releases what the pause withheld: after it stores `paused`, it calls `ConsumerState::maybe_flow` on the same slot guard and stages the `CommandFlow` that call returns, exactly as `pop_message` does.
`maybe_flow` already returns `None` while paused and below the half-queue threshold, so a pause, a resume with nothing owed, a resume of a consumer that is closed, mid-seek or has no receiver queue, and a resume of an unknown handle all stage nothing; only a resume that owes at least the threshold stages one flow, carrying `consumed_since_flow`, and a second resume stages nothing because the ledger was reset.

The tokio and moonpool `Consumer::resume` now call `Connection::set_paused(handle, false)` under the connection mutex instead of writing the slot field, then wake the driver as before.
Both runtimes change identically, and `pause()` is unchanged: it is a per-slot write that stages nothing.
The two `Consumer::resume` bodies were the only production writers of `paused = false`; the only other production writes set `paused = true` (`pause()`, and the manual-FLOW fence of a scalable-topic segment consumer, which keeps a zero receiver queue and so never emits a flow whatever `paused` says).

- The lock order is global → per-slot (ADR-0038): `resume` takes the connection mutex and `set_paused` takes the slot's inside it.
  Before this change `resume` took the slot lock alone.
  `MultiTopicsConsumer::resume` calls it while holding its own consumer-list lock, the nesting `is_connected` (`shared.inner.lock()`) and `redeliver_unacked` (through `negative_ack_many`) already use in both runtimes.
  No in-tree caller reaches `resume` with the connection or a slot lock held: the sans-io core calls no user code but wakers, which it fires after releasing the slot lock, and a message listener runs on its own task after `receive().await` returned.
- The `ConsumerApi::resume` contract in `crates/magnetar/src/engine/mod.rs` states the release.
- No wire change, no new dependency, and no change to a signature: `Connection::set_paused` and `Consumer::resume` keep theirs.

## Consequences

- A consumer paused while its receive loop drained the window is no longer stranded by `resume()`: the flow is staged at the resume, the driver is woken in the same call, and the parked `receive()` is delivered the message the broker dispatches against it.
- A resume releases at most what the consumer owes the broker, in one `CommandFlow` that carries `consumed_since_flow`, and clears it, so a repeated resume releases nothing twice.
- Cost: `resume` takes the connection mutex, which `pause` and `is_paused` still avoid; it is a user-driven control call, not a per-message path.
- A resume across a reset releases nothing: `Connection::reset()` zeroes `consumed_since_flow`, `granted_permits` and `permit_balance` and clears the outbound buffer, and the re-attach's `initial_flow` grants the fresh window.
- A `receive()` first polled after a resume was already served by the `maybe_flow` in `pop_message`; the flow is now out before that poll, so it no longer waits for it.
- Not changed here: `Connection::initial_flow` does not look at `paused`, so a consumer subscribed while paused is still granted its first window.
- Incompatible with: a caller that relied on a resumed consumer staying silent until its next `receive()` or arrival, which the stranded state made indistinguishable from a hang.

### ADR-0024 test layers

(a) `magnetar-proto/src/conn.rs` `consumer_stall_and_recovery_tests::resuming_a_drained_paused_consumer_releases_the_flow_it_owes_the_broker`; (b) `magnetar-runtime-tokio/src/consumer.rs` `tests::resume_releases_the_flow_owed_to_a_receive_parked_during_the_pause`; (c) the same test in `magnetar-runtime-moonpool/src/consumer.rs`; (d) `magnetar-differential/tests/consumer_resume_flow_equivalence.rs`.
End-to-end: `crates/magnetar/tests/e2e_consumer_resume_flow.rs` pauses a consumer against a real broker, drains its window, parks a `receive()` and resumes; the parked receive must get the next message, and the consumer must keep flowing after it.

## References

- `crates/magnetar-proto/src/conn.rs` — `Connection::set_paused`, `Connection::pop_message`, `stage_flow`.
- `crates/magnetar-proto/src/consumer.rs` — `ConsumerState::maybe_flow`, `is_stall_candidate`.
- `crates/magnetar-runtime-tokio/src/consumer.rs` and `crates/magnetar-runtime-moonpool/src/consumer.rs` — `Consumer::resume`.
- [ADR-0038](0038-split-connection-mutex.md) — the lock order `resume` now follows.
- [ADR-0101](0101-consumer-stall-detection-and-in-place-recovery.md) — why the watchdog cannot release the flow of a consumer holding no permit.
- [ADR-0107](0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md) — the flow ledger (`consumed_since_flow`, `permit_balance`) the release reads.
