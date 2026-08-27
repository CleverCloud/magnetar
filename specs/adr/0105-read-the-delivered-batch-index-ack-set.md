# ADR-0105 — Read the delivered batch-index ack_set on the receive path

- **Status**: Accepted (amends [ADR-0096](0096-reconnect-batch-and-durable-cursor-safety.md), which resolves its deferred receive-side `ack_set` gap)
- **Date**: 2026-08-27
- **Decider**: Florentin Dubois
- **Tags**: consumer, batch, flow-control, ack-timeout, pip-54, sans-io, issue-436

## Context

[Issue #436](https://github.com/CleverCloud/magnetar/issues/436): twelve `Shared` consumers over a twelve-partition topic whose broker entries pack 1024 messages each.
Within the hour every consumer sits at `availablePermits` `0` or negative — one reached `-3535` — `msgRateOut` is `0`, `msgRateRedeliver` is `0`, the subscription is NOT blocked on unacked messages, and the application's own probe reports it acknowledging essentially everything it receives.
Raising `ack_timeout` past the run length, with nothing else changed, makes the wedge disappear entirely.
The same report carries a second symptom: one batched entry pinned the subscription's mark-delete position for days, with the first individually-deleted range starting exactly one entry past it.

Both are the same defect, and it is a field magnetar never reads.

`pb::CommandMessage.ack_set` (`repeated int64`) is the broker's per-position view of a batched entry it is dispatching: bit `i` SET ⇒ position `i` is still unacked.
It is the same bitset convention as the outbound `BatchAckEntry`, and the broker attaches it exactly when `acknowledgmentAtBatchIndexLevelEnabled=true` and it is re-dispatching an entry some of whose positions have already been acknowledged.
`ConsumerState::deliver`'s batched branch has never looked at it: it explodes `0..num_messages_in_batch` unconditionally and stamps `BatchAckEntry::fresh(num_in_batch)` — an all-unacked bitset — whenever the tracker has no entry for `(ledger_id, entry_id)`.

Three consequences compound into a metastable loop.

- **The application is handed messages it already acknowledged.** A 1024-message entry with 1022 positions acked re-delivers all 1024. An application that de-duplicates has nothing to do with 1022 of them and spends its receive capacity re-consuming its own history; one that does not, double-processes.
- **`classify_and_queue` re-registers every one of them in the ack-timeout tracker.** The next sweep therefore re-requests positions the broker has already accounted for, the broker honours the request by re-dispatching the entry, the client re-registers them again, and the redelivery request set never converges. This is why raising `ack_timeout` past the run length hides the wedge: without a sweep there is no second dispatch, and nothing feeds the loop.
- **The permit mirror is debited for positions the broker never charged.** With `acknowledgmentAtBatchIndexLevelEnabled=true` the broker debits `MESSAGE_PERMITS_UPDATER.addAndGet(this, ackedCount - totalMessages)` as it re-dispatches (apache/pulsar master `3bf3ec2`, `Consumer.sendMessages`, `Consumer.java:433-434`) — it charges the subscription `totalMessages - ackedCount`, only the positions it still expects to be consumed. `classify_and_queue` calls `record_dispatch_unit` once per exploded position, so the mirror debits `totalMessages`. Every re-dispatch drives it `ackedCount` further below the broker's real `availablePermits`. The drift is unbounded and one-way, which is how a consumer reaches `permits=-3535`, and the client stops replenishing long before the broker is actually out.

The Java client does not have this problem, and the reason it does not is instructive: `ConsumerImpl.receiveIndividualMessagesFromBatch` skips the cleared positions **and deliberately keeps them out of `skippedMessages`**, so `increaseAvailablePermits` never fires for them — "Broker … did not decrease the permits in the broker-side. So do not acquire more permits for this message" (`ConsumerImpl.java:1798-1862`).
Neither side moves for a skipped position: the broker charged nothing, the client hands nothing back.

The mark-delete symptom is the tracker half of the same omission.
A session whose first sight of an entry is a re-dispatch seeds `BatchAckEntry::fresh` — all-unacked — so acknowledging the genuinely outstanding positions still leaves the already-accounted ones set.
The entry never reaches "fully acked", every `CommandAck` for it carries an `ack_set` that re-asserts positions the broker considers deleted, the broker holds the cursor behind it, and the tracker entry leaks for the lifetime of the connection — the same unbounded-growth class as issue #326.

[ADR-0096](0096-reconnect-batch-and-durable-cursor-safety.md) built the conservative ACK-TIME reconstruction for the mirror-image case: a reset has cleared session-local PIP-54 state, an individual ack arrives for a batch the tracker no longer knows, and the client rebuilds an all-unacked entry and clears only the position explicitly acknowledged, because Pulsar intersects that `ack_set` with its persisted cursor state and an absent bitset would be read as a full-entry ack.
That reasoning is about an ack for which the client has no broker-supplied evidence at all.
It says nothing about a delivery that carries the broker's own bitset, and ADR-0096 deliberately left the receive side out of scope.
This ADR fills exactly that hole.

### Alternatives considered

- **Skip the acked positions but keep charging a permit for them.** Fixes the duplicate delivery and the ack-timeout loop, leaves the accounting drift. Refused: the drift is the half of issue #436 that reaches `availablePermits = 0` and stops the subscription, and `Consumer.java:433-434` is unambiguous about what the broker charged.
- **Skip them and hand their permits back through `record_broker_permit_consumed`.** The symmetric error in the other direction: it would credit a broker that never debited, so the client's grants would run ahead of the broker's counter instead of behind it. This is precisely the case `ConsumerImpl.java:1798-1862` calls out.
- **Overwrite the tracker entry with the delivered bitset instead of AND-accumulating.** Refused: a `CommandAck` this session issued between the dispatch and the re-dispatch may not yet be reflected in the bitset the broker sends, and overwriting would re-assert that position as unacked. `ManagedCursorImpl.java:2632-2645` accumulates by AND for the same reason; bits must only ever clear.
- **Reconstruct the delivered state at ack time rather than at delivery time**, extending ADR-0096's `or_insert_with(BatchAckEntry::fresh)`. Refused: it cannot fix the delivery or the permit halves at all, since both are decided before any ack exists, and it would put a second, divergent reading of the same bitset next to the conservative one ADR-0096 deliberately reasoned about. ADR-0096's ack-time reconstruction is unchanged by this ADR.
- **Gate the whole behaviour behind a client option.** Refused: the field is absent unless the broker chose to send it, so the correct behaviour is already inert wherever it does not apply — a flag would only give an operator a way to keep the bug.

## Decision

`ConsumerState::deliver`'s batched branch reads `cmd.ack_set` once, up front, into a single `BatchAckEntry` describing which positions the broker still lists as unacked, and uses that one bitset for all three decisions.

- **Construction** — `BatchAckEntry::from_delivered_ack_set(batch_size, ack_set)` starts from `BatchAckEntry::fresh(batch_size)` and ANDs in each delivered word. A word the broker did not send reads as `u64::MAX`, so a **missing or short** `ack_set` leaves every position it does not cover UNACKED: a truncated bitset may never acknowledge a position on the broker's behalf. Bits at or above `batch_size` are never set by `fresh` and this only clears, so the tail stays zero and `is_fully_acked` stays reachable. An **empty** `ack_set` therefore reduces to exactly `fresh` — by construction, not by a special case — which is what keeps a first delivery, and every delivery from a broker running `acknowledgmentAtBatchIndexLevelEnabled=false`, byte-for-byte unchanged.
- **Delivery** — a position whose bit is CLEAR is decoded and dropped. The wire body is still walked position by position: the payload cursor has to advance through a skipped sub-message for the ones behind it to parse at all, so "skip" means "decode and drop", never "stop parsing". Skipping means skipping `classify_and_queue`, the single site that queues the message, registers it with the ack-timeout tracker, and calls `record_dispatch_unit` — so the application never sees it, the sweep never re-requests it, and no permit is debited for it. Nothing is handed back either: `consumed_since_flow` only moves on `pop_message`, so a position that never entered the queue never enters the flow ledger.
- **Tracker** — a VACANT `(ledger_id, entry_id)` entry is seeded from the delivered bitset; an OCCUPIED one AND-accumulates it (`existing &= delivered`). Bits only ever clear, in both directions: a position this session acked locally is never re-asserted as unacked by an older broker view, and a position the broker reports as acked clears even if this session never acked it — on a `Shared` subscription a sibling consumer did.
- **Outcome** — the branch returns `DeliverOutcome::Delivered { count }` where `count` is the number of positions actually queued. `count == 0` (every position acked) is a legitimate outcome and needs no caller change: `conn.rs`'s dispatch computes `start = queue_len.saturating_sub(count)` and iterates `start..queue_len`, which is empty.
- **Scope** — `num_messages_in_batch <= 1` never reaches this branch, so a stray `ack_set` on an unbatched entry is ignored exactly as before.

Nothing here reads a clock (ADR-0011) or can panic (invariant #6): `BatchAckEntry::is_unacked` saturates an out-of-range position to a word index no bitset holds, and every shift is masked to `0..64` by construction.

[ADR-0096](0096-reconnect-batch-and-durable-cursor-safety.md)'s conservative ACK-TIME reconstruction in `Connection::ack` is untouched and remains binding verbatim.
What this ADR resolves is the receive-side gap ADR-0096 left open: at delivery the client now has the broker's own bitset, so it no longer has to assume all-unacked there.
The two are complementary — ADR-0096 governs an ack with no broker evidence, this ADR governs a delivery that carries it — and the seed this ADR installs is what makes ADR-0096's `or_insert_with` reconstruction a genuine fallback rather than the common case for a re-dispatched entry.

## Consequences

**Easier.** The metastable amplification loop is gone: an ack-timeout sweep on a partially-acked batched entry re-requests the same outstanding positions on every cycle instead of a set that grows by `ackedCount` each time, so a de-duplicating application converges. The permit mirror tracks the broker's `availablePermits` across re-dispatches instead of drifting one-way below it, so `available_permits()` stays a usable signal and the ADR-0101 stall signature stays measured against the right number. A partially-acked entry now completes, so the subscription's mark-delete position advances past it and the tracker entry drops out instead of leaking.

**Harder / cost.** One `BatchAckEntry` — `batch_size.div_ceil(64)` `u64` words, 16 for the 1024-message entries of issue #436 — is built per batched delivery and cloned once when the tracker entry is vacant. Negligible against the per-position `IncomingMessage` construction it sits beside, and it replaces the `BatchAckEntry::fresh` allocation the branch already made.

**Behaviour that changes for an existing deployment.** Only where the broker actually sends `ack_set`, i.e. `acknowledgmentAtBatchIndexLevelEnabled=true` re-dispatching a partially-acked batched entry. There, an application that today receives duplicates of positions it already acknowledged stops receiving them. That is the fix, but it is a visible delivery change: an application that was relying on the duplicates to re-drive side effects will stop seeing them. A broker with the feature disabled never attaches the field, so its behaviour is bit-for-bit unchanged — the empty-`ack_set` path is the pre-existing one.

**Incompatible with** any future reading of `CommandMessage.ack_set` as "positions to REDELIVER" rather than "positions still unacked". The bit convention is shared with the outbound `BatchAckEntry` and with `ManagedCursorImpl`; inverting it anywhere inverts it everywhere.

**Not covered.** A broker that sends a bitset inconsistent with the entry it is dispatching is handled conservatively (uncovered positions delivered, never silently dropped) but not detected or reported — there is no wire signal that distinguishes a malformed bitset from a legitimately narrow one.

Ships the ADR-0024 five layers: `magnetar-proto` unit tests for the skip, the permit debit, the seed, the AND-merge, the short-`ack_set` and all-clear edges and the unbatched no-op; the 1:1 mirrored `batch_redelivery_flow_wedge.rs` in both engines; the `batch_redelivery_flow_equivalence.rs` differential trace over a scripted broker that models per-message permits with `ackedCount` refund, flag-true `ack_set` attachment with AND accumulation, and `Shared` redelivery routing; and `e2e_batch_ack_timeout_shared.rs` against a real broker pinned to `acknowledgmentAtBatchIndexLevelEnabled=true`.

## References

- [`crates/magnetar-proto/src/consumer.rs`](../../crates/magnetar-proto/src/consumer.rs) — `ConsumerState::deliver`'s batched branch, and `BatchAckEntry::from_delivered_ack_set` / `intersect_delivered` / `is_unacked`.
- [`crates/magnetar-proto/src/conn.rs`](../../crates/magnetar-proto/src/conn.rs) — the `DeliverOutcome::Delivered { count }` dispatch that tolerates `count == 0`, and the untouched ADR-0096 ack-time reconstruction.
- [`crates/magnetar-differential/src/broker.rs`](../../crates/magnetar-differential/src/broker.rs) — the scripted broker's batched entries, per-message permits, per-entry unacked bitset, and `Shared`-routed ack-timeout redelivery.
- [ADR-0096](0096-reconnect-batch-and-durable-cursor-safety.md) — the conservative ack-time reconstruction, whose deferred receive-side gap this resolves.
- [ADR-0011](0011-clock-injection-sans-io.md) — the injected clock this branch keeps carrying.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the five test layers this ships with.
- apache/pulsar master `3bf3ec2` — `Consumer.java:433-434` (`ackedCount - totalMessages` permit debit), `ConsumerImpl.java:1798-1862` (`receiveIndividualMessagesFromBatch` skip, and why it acquires no permits for a skipped position), `ManagedCursorImpl.java:2632-2645` (AND accumulation of a delivered bitset).
