# ADR-0138 — Forget abandoned transaction requests and reclaim coordinator entries nobody can finish

- **Status**: Accepted (amends the entry retention of the transaction coordinator described in [ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md): a refused outcome keeps the entry only while a participant that could still accept it is alive; the rest of ADR-0120 remains binding. Bounds the per-request `TxnClient` record that [ADR-0129](0129-bound-and-fast-fail-pip-31-transaction-requests.md) left as a residual to the session that carried the request)
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: transactions, resource-leak, proto, facade

## Context

Two transaction paths kept state for ever once the party that could use it was gone.

**Transaction requests of a session that ended.**
[ADR-0129](0129-bound-and-fast-fail-pip-31-transaction-requests.md) made `new_txn`, `add_partition_to_txn` and `add_subscription_to_txn` cancellable, made the response arms park an outcome only for a request still pending, and kept the transaction-coordinator client consuming a late reply, so that a registration the broker did perform stays recorded on the local transaction.
It named one residual: the embedded `TxnClient` keeps a per-request record for a request that is cancelled and never answered, also after `Connection::reset`.
`Connection::reset` turned every pending request into a `SessionLost` outcome but never touched the `TxnClient`, whose per-request maps and waker slabs (`new_txn_by_request`, `add_partition_by_request`, `add_subscription_by_request`, `end_txn_by_request` and their slabs) are emptied only by the matching response, which a dead session never sends.
Every reconnect therefore left, for the life of the connection, one entry for each transaction request pending at the reset and one for each request cancelled earlier on that session, for instance one whose operation deadline fired during the outage before the supervisor redialled.

**Coordinator entries whose participants are gone.**
`TransactionCoordinator` removes an entry only when `complete` finishes with every participant having accepted the outcome.
A participant that refuses (a scalable aggregate that was closed while its handle was still alive) keeps the entry of a confirmed transaction on purpose, so a retried commit resumes without a second `EndTxn`.
Once the participant's handle is dropped nothing can ever accept the outcome, yet the entry stayed with a dead `Weak` in it: dead weaks were pruned only inside `prepare`, which needs a further `commit_transaction` on that very transaction.
The `TransactionParticipant` documentation said such weaks "disappear naturally after close/drop", which they did not.

Alternatives considered:

1. **Also forget the entry in `Connection::cancel_request`.** Not done: ADR-0129 deliberately keeps the late reply of a cancelled request flowing into the `TxnClient` while its session lives, so that a registration the broker performed is recorded locally, and that reply removes the entry.
2. **Forget, on a reset, only the requests the reset resolves.** Rejected: a request cancelled before the reset has already left `pending_requests`, so its entry would stay for the life of the connection.
3. **Reclaim a coordinator entry from the participant's `Drop`.** Rejected: it adds a back-reference from the aggregate's user guard to the client's coordinator for a one-line sweep that a new transaction can do.
4. **Evict never-ended transactions after their timeout.** Not done: it is a product decision (what a late commit of an evicted transaction should answer), not a leak fix.

## Decision

- `Connection::reset` retires every per-request entry and waker slot of the `TxnClient` (`TxnClient::forget_all_requests`), once, after it has resolved the pending requests: those of the requests it resolves with `SessionLost`, `EndTxn` included, and those of the requests whose caller was cancelled earlier on the same session.
  This keeps ADR-0129's decision: the late reply ADR-0129 hands to the `TxnClient` can only arrive on the session that carried the request, the reset clears the connection's buffers, and the next session's broker never saw the id.
  An `EndTxn` outcome is retained across a reset so a retried commit or abort resumes the canonical request, and that resume goes through the connection's `EndTxn` maps and the retained outcome, never through the `TxnClient` entry.
- `Connection::cancel_request` is unchanged: on a live session the late reply of a cancelled request still reaches the `TxnClient` (ADR-0129), which removes the entry then.
- `TransactionCoordinator::open` sweeps: it drops every entry whose gate is `Committed` or `Aborted`, with no end in flight, and whose remaining participants are all dead (`Weak::strong_count() == 0`).
  An open transaction, a closing or issued one and an `Unknown` one are never swept: they can still be ended, and an `Unknown` one needs its retry to reach the coordinator.
  A refusing participant that is still alive keeps the entry, as before.

## Consequences

- After a reset the `TxnClient` holds no per-request entry of the ended session, and nothing reads one again: no reply to a request of that session can reach the next one.
- A retried `commit_transaction` or `abort_transaction` on a transaction whose entry was swept answers `UnknownTransaction`, exactly like a retry after a fully successful end; the first call had already returned the coordinator's state, with only a warning about the participant.
- The sweep costs one pass over the open transactions per `new_transaction`, each taking the entry's state lock briefly. The lock order is the entry map, then one entry's state; no path takes the map while holding a state.
- **Not addressed.**
  - A transaction the application never ends keeps its coordinator entry for the life of the client; bounding that needs the eviction decision above.
  - A cancelled transaction request whose reply never comes keeps its `TxnClient` entry until its session ends, as ADR-0129 accepted.
  - An `EndTxn` outcome kept after a cancelled waiter that the application never retries stays, by the retry contract.
  - `TxnClient` keeps one `TransactionMetadata` record for every transaction ever opened, ended or not: no production path calls `TxnClient::forget`. This change does not alter that.
  - Every transaction response arm pushes a `ConnectionEvent::TxnResponse` with a bare `push_back`, past the observational cap of the event queue, and nothing in the workspace drains that event, so those events accumulate until the next reset. This change does not alter that.
- Test layers, as they are:
  - (a) `magnetar-proto`: `txn_request_reset_retires_the_per_request_client_state`, for a request pending at the reset and for one cancelled before it.
  - (b) and (c) `txn_request_redial::txn_request_timed_out_during_an_outage_leaves_no_txn_client_entry_after_the_redial` in each runtime: a supervised redial after an `add_partition_to_txn` whose `operation_timeout` expired during the outage.
  - (d) `txn_request_cancel_equivalence.rs`: a cancelled call through the façade trait on both engines, and on both engines' connections the cancel of the three non-`EndTxn` kinds, the reset of all four, and the reset of the three after a cancel.
  - The sweep has no proto or runtime layer, the coordinator being façade code shared by both engines: `entry_kept_for_a_refusing_participant_is_reclaimed_once_the_participant_is_dropped` is the unit test and `dropped_refusing_participant_entry_is_reclaimed_equivalently` the differential one.
  - No `e2e_*.rs` test was added: a real broker reaches the reset path (a reconnect with a transaction request in flight or timed out) but not deterministically.

## References

- `crates/magnetar-proto/src/conn.rs` — `reset`.
- `crates/magnetar-proto/src/txn.rs` — `TxnClient::forget_all_requests`.
- `crates/magnetar/src/transaction.rs` — `TransactionCoordinator::open`.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) (test layers), [ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md), [ADR-0129](0129-bound-and-fast-fail-pip-31-transaction-requests.md).
