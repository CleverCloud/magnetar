# ADR-0129 — Bound and fast-fail the PIP-31 transaction requests

- **Status**: Accepted (extends [ADR-0059](0059-terminal-fast-fail-new-ops.md)'s entry-point guard list and [ADR-0080](0080-configurable-operation-retry-policy.md)'s operation-deadline scope to the transaction request family) — the per-request `TxnClient` record of a request cancelled and never answered now lasts until its session ends, because `Connection::reset` retires it ([ADR-0138](0138-forget-abandoned-transaction-requests-and-reclaim-unfinishable-coordinator-entries.md)); everything else below remains binding.
- **Date**: 2026-11-19
- **Decider**: Florentin Dubois
- **Tags**: transactions, resilience, timeouts, runtime, sans-io

## Context

Every caller-visible setup operation of the runtimes is bounded by `operation_timeout` ([ADR-0080](0080-configurable-operation-retry-policy.md)) and fast-fails with `ClientError::PeerClosed` once its connection is terminal ([ADR-0059](0059-terminal-fast-fail-new-ops.md)).
The PIP-31 request family was left out of both:

- `Client::new_txn` once the transaction coordinator is bootstrapped, `add_partition_to_txn`, `add_subscription_to_txn` and `end_txn` registered a pending request and awaited its `RequestFut` bare.
  They ran no `fail_if_no_driver()` check, and nothing raced them against `operation_timer()`.
  Only the coordinator bootstrap's lookup was covered, and only until `txn_bootstrapped` latched.
- After a genuinely terminal exit, `Connection::fail_all_pending` has already run and no driver is left, so a request registered afterwards is never answered.
  `Transaction::commit` and `abort` then waited forever on a dead client while `send`, `subscribe` and `lookup` failed fast.
- During a supervised outage the socket is dead, the supervisor backs off with no attempt cap by default, and `mark_disconnected` leaves pending requests alone.
  A commit issued then waited for the whole outage and finished with `unexpected end_txn outcome: SessionLost`.
- A coordinator that stays silent on a healthy connection never answers either: no sweep in `handle_timeout` covers the five request kinds.
- `TxnError::Timeout`, documented as "the driver layer surfaces it", was never constructed.

The same bare await existed in three more places: the `TransactionApi` implementation the façade carries for the moonpool engine (a third copy of the four methods, which also lacked the `Terminal` outcome arm), and the `add_subscription_to_txn` helper of each runtime's scalable-topic `SegmentSubscriber`.

## Decision

Each PIP-31 request is guarded by ADR-0059's check and bounded by one operation deadline, on both engines 1:1 ([ADR-0024](0024-cross-runtime-test-and-coverage-policy.md)).

- **Guard.** `new_txn`, `add_partition_to_txn`, `add_subscription_to_txn`, `end_txn`, the coordinator handshake (`TcClientConnect`) and the scalable `add_subscription_to_txn` call `fail_if_no_driver()` before they register anything.
  In `end_txn` it precedes `Connection::end_txn`, so no waiter lease and no canonical record is created for a request that cannot be answered.
- **Deadline.** Each public call takes one `operation_timer()` and races its reply against it, deadline arm first.
  `new_txn` shares that one deadline between the coordinator bootstrap (lookup and `TcClientConnect`) and the `NewTxn` request; each step races it deadline arm first, so an operation that expires during one step fails there and enqueues no further command.
  A deadline that elapses between two steps is reported by the next step's race, after that step's command has been written; that request is then cancelled, as when the deadline elapses just after a send.
  Expiry is the engine's ordinary operation-deadline error (`ClientError::Timeout` on Tokio, the `Other` timeout message on Moonpool, which has no `Timeout` variant).
  `TxnError::Timeout` stays unconstructed: the engines return `ClientError`, and the façade stringifies it, so the structured variant had no reader.
  Its documentation now says so instead of promising a producer.
- **Cancellation.** The expired `TcClientConnect`, `NewTxn`, `AddPartitionToTxn` and `AddSubscriptionToTxn` requests are cancelled through `RequestFut::cancellable`, which retires their correlation state ([ADR-0080](0080-configurable-operation-retry-policy.md) § Cancellation owns all local correlation state).
  `Connection` now also ignores the late broker reply to such a request: the four response arms park an outcome only when the request is still in `pending_requests`.
  Before, the reply of a cancelled request parked an `outcomes` entry nobody would ever take.
  The transaction-coordinator client still consumes the reply, so a registration the broker did perform stays recorded on the local transaction.
- **`end_txn` is not cancelled.** An expired `end_txn` returns the timeout error and releases its waiter lease only.
  The canonical `EndTxn` request stays pending, because the outcome of the commit or abort is unknown and a retry with the same arguments must resume that request rather than send a second command (`Connection::release_end_txn_waiter`).
- **One implementation per engine.** The façade's moonpool `TransactionApi` delegates all four methods to `magnetar_runtime_moonpool::Client`, like the Tokio one does.
  The scalable `add_subscription_to_txn` of each runtime goes through the same `await_txn_outcome` helper, with the subscriber's own `operation_timeout` and, on Moonpool, its injected sleep provider.

## Consequences

- A transaction request issued on a terminal connection fails with `PeerClosed` synchronously, and one on a live connection with a silent coordinator fails within `operation_timeout`, instead of waiting for an outage that may not end.
- `Transaction::commit` can now fail with a timeout while the broker later commits: the caller must treat that error as an unknown outcome and retry `end_txn` (which resumes the pending request) rather than assume an abort.
- A stream consumer's transaction registration that times out is recorded as failed for that source, like any other registration error.
- The check in front of the lock is the same two-condition gate as everywhere else; a request registered in the instant between the check and a terminal exit is bounded by the deadline instead.
- Residual, not changed here: the transaction-coordinator client keeps a small per-request record for a request that is cancelled and never answered (also after `Connection::reset`), and a late `EndTxn` reply with no retry stays parked, which is the resume design.
- ADR-0024 layers: a `magnetar-proto` unit test for the late reply, and a Tokio and a Moonpool integration test for the terminal fast-fail and one for the deadline (1:1).
  `magnetar-differential` runs the late-reply contract against the shared `Connection`, the façade's transaction registration calls on both engines, and a stream consumer's transaction registration that the coordinator never answers, which times out the same way on both engines.
  No `e2e_*.rs` test was added: `e2e_transactions.rs` drives the changed methods on their success path and cannot script a dead client or a silent coordinator.

## References

- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — four-layer test and 1:1 parity policy.
- [ADR-0059](0059-terminal-fast-fail-new-ops.md) — the entry-point guard this ADR extends.
- [ADR-0080](0080-configurable-operation-retry-policy.md) — the operation deadline and cancellation ownership this ADR extends.
- `crates/magnetar-runtime-tokio/tests/terminal_exit.rs`, `crates/magnetar-runtime-moonpool/tests/terminal_exit.rs` — `txn_ops_after_terminal_drop_fail_fast`.
- `crates/magnetar-runtime-tokio/tests/txn_operation_deadline.rs`, `crates/magnetar-runtime-moonpool/tests/txn_operation_deadline.rs` — `txn_requests_honour_operation_timeout`.
- `crates/magnetar-proto/src/conn.rs` — `late_reply_to_a_cancelled_txn_request_leaves_no_outcome`.
- `crates/magnetar-differential/tests/txn_late_reply_contract.rs` — `late_reply_to_a_cancelled_txn_request_leaves_no_outcome`.
- `crates/magnetar-differential/tests/facade_transaction_registration_equivalence.rs` — `transaction_registration_is_equivalent_on_both_facades`.
- `crates/magnetar-differential/tests/scalable_runtime_surface_equivalence.rs` — `unanswered_transaction_registration_times_out_equivalently`.
