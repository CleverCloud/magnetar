# ADR-0132 — Address PIP-145 topic-list deltas to the watcher they belong to

- **Status**: Accepted (amends the single-buffer delta surface that [ADR-0037](0037-multi-topics-pattern-consumer-pass-2-lift.md) lifted into `BrokerMetadataApi`; the rest of ADR-0037 remains binding; amended 2026-11-20 — the reply to a cancelled snapshot records nothing)
- **Date**: 2026-11-19
- **Decider**: Florentin Dubois
- **Tags**: pattern-consumer, pip-145, topic-watcher, proto, runtime, facade

> **Amendment (2026-11-20, the reply to a cancelled snapshot).** The Decision below says that a request cancelled before its response arrived has no pending entry, and that the id the broker echoed then stands in for the outcome nobody reads.
> That outcome was never taken and `reset()` does not clear `outcomes`, so each one held the broker's whole topic list for the life of the connection.
> A `CommandWatchTopicListSuccess` is now recorded only while its request is still pending as a `TopicWatcher`, and the id comes from that pending entry: the reply to a cancelled snapshot records no outcome, queues no event and leaves the closed watcher alone.
> The broker-side watcher of that snapshot stays open, as before ([`docs/follow-ups.md`](../../docs/follow-ups.md) §31), and a new snapshot on the same connection is a new request that resolves on its own reply.
> `topic_list_watcher_contract.rs` follows: its cancelled-watch case now asserts that no outcome is recorded instead of the echoed id.
> The rest of this ADR is unchanged.

## Context

Every `PatternConsumerBuilder::subscribe` opens its own broker-side topic-list watcher on the client's connection, and the connection allocates it an id (`Connection::watch_topic_list`, `next_watcher_id`).
The broker pushes a `CommandWatchTopicUpdate { watcher_id, … }` to one of them when a topic matching that watcher's pattern is created or deleted.
The connection used `watcher_id` only to refresh the watcher's `topics_hash`, then emitted `ConnectionEvent::TopicListChanged { added, removed }` with no id at all, and emitted it even for an id it holds no watcher for.
Each runtime driver appended the event to one buffer per client (`ConnectionShared::topic_list_changes`), and `BrokerMetadataApi::poll_topic_list_change` popped that buffer for whoever asked.

Two pattern consumers on one client therefore shared one queue:

- the consumer that called `update` first applied a delta that belonged to the other watcher, and subscribed its own template to a topic outside its pattern, which is a durable subscription created on the broker;
- the owner of that delta never saw it, because discovery is delta-only after the first snapshot and the broker announces a topic once.

A second watcher on the client does not need a second pattern consumer: `PulsarClient::topic_list_snapshot` opens one too, and a snapshot cancelled after its frame went out leaves the broker watcher live with no local entry.

Alternatives considered:

1. **Filter the added topics against the consumer's own pattern.** Rejected: the broker applies Java regex semantics and the client deliberately does not re-validate matches (`pattern_consumer.rs`, module doc), so a local filter would be a second, divergent regex engine; and it would still drain the other consumer's delta out of the buffer.
2. **One buffer per watcher in `ConnectionShared`.** Rejected: it changes the public `topic_list_changes` field both runtimes expose, which `next_topic_list_change` (any watcher) and the waiter tests read directly, for no behaviour the filtered take below does not give.
3. **Carry the watcher id end to end and take by id.** Chosen.

## Decision

A delta is addressed to the watcher it belongs to from the wire to the consumer that applies it.

- `ConnectionEvent::TopicListChanged` carries `watcher_id`, and so does `OpOutcome::TopicListSnapshot`, which hands the id the connection allocated to the caller that opened the watcher. The id is read from the watcher's pending request; only a request cancelled before its response arrived has no pending entry, and then the id the broker echoed stands in (nobody reads that outcome).
- The connection emits `TopicListChanged` only for a watcher it holds.
  A delta for an id it does not hold (a cancelled snapshot, a stale id) has no owner that could ever claim it, so it is dropped instead of queueing for the life of the connection.
- Both runtimes keep `ConnectionShared::topic_list_changes` as one ordered buffer, and `TopicListChange` gains `watcher_id`.
  `ConnectionShared::take_topic_list_change(watcher_id)` removes the oldest delta of one watcher and leaves every other watcher's deltas, and each watcher's own order, in place.
  `Client::poll_topic_list_change` takes the watcher id and delegates to it; `Client::next_topic_list_change` stays the any-watcher wait.
- `Client::watch_topic_list_with_operation_deadline` (the hidden deadline seam) resolves to `(watcher_id, topics)`; `watch_topic_list` still returns the topics only.
- In the façade, `BrokerMetadataApi::watch_topic_list_with_deadline` resolves to the id and the snapshot (`WatchTopicListWithIdFut`), and `BrokerMetadataApi::poll_topic_list_change` takes the id.
  `PatternConsumer` keeps the id its builder received and `update` takes only the deltas addressed to it.
  The façade `TopicListChange` is unchanged: the id is the argument of the poll, not a field of the result.

## Consequences

- Two pattern consumers on one client each reconcile from their own deltas, in either order of `update`; `crates/magnetar/tests/pattern_watcher_delta_isolation.rs` pins both directions over the tokio and the moonpool engine.
- API surface: `TopicListChange` (both runtimes) gains a public field, `Client::poll_topic_list_change` gains a parameter, `BrokerMetadataApi::poll_topic_list_change` and `watch_topic_list_with_deadline` change signature, and `watch_topic_list_with_deadline` loses its default body (a default cannot know the id).
  That is a source break for an external implementor of `BrokerMetadataApi` or a caller of the poll; both in-tree engines implement it, and the in-tree callers are the pattern consumer and its builder.
- A watcher nobody polls is not drained any more: the deltas of a `topic_list_snapshot` watcher, of a dropped `PatternConsumer` and of a pattern consumer whose build failed after the watch opened stay in the buffer for good: nothing clears `topic_list_changes`, so they accumulate.
  Before this change the next `update` of any consumer drained and applied them, which is the defect; the underlying cause, that no `CommandWatchTopicListClose` is sent so the broker keeps pushing, is unchanged and is not addressed here.
- A caller that passes `update` a different client than the one that built the consumer polls a connection its id does not belong to; ids are per connection and start at 0, so the call can read another watcher's deltas there.
  `PatternConsumer::update` now documents that `client` must be the one that built the consumer.
- ADR-0024 layers, stated as they are rather than as the policy would have them: (a) `topic_list_deltas_and_snapshots_carry_their_watcher_id` and `topic_list_delta_for_a_watcher_this_connection_does_not_hold_is_dropped` in `magnetar-proto`; (b) and (c) are unit tests, not integration tests: `take_topic_list_change_takes_only_the_addressed_watchers_deltas` in each runtime's `lib.rs` (which `check-runtime-test-parity` keeps 1:1) exercises the filtered take on `ConnectionShared` without a driver or a broker, and the changed driver arm of each runtime is reached only through the façade scenarios of `crates/magnetar/tests/pattern_watcher_delta_isolation.rs`, which `check-sim-coverage` does not run.
  In `magnetar-differential`, `facade_pattern_watcher_delta_isolation.rs` and `facade_pattern_concurrent_update_single_child.rs`, the same scenarios as the two façade files named in the References without their feature gates, reach the two driver arms and the proto `WatchTopicListSuccess` and `WatchTopicUpdate` arms on both engines, and `topic_list_watcher_contract.rs` drives `Connection` directly for the snapshot of a live and of a cancelled watch; with them `check-sim-coverage` covers every added line in both coverage domains.
  No `e2e_*.rs` test was added.

## References

- `crates/magnetar-proto/src/conn.rs` — `WatchTopicListSuccess` and `WatchTopicUpdate` arms.
- `crates/magnetar-proto/src/event.rs`, `crates/magnetar-proto/src/conn_types.rs` — `TopicListChanged`, `OpOutcome::TopicListSnapshot`.
- `crates/magnetar-runtime-tokio/src/{lib,driver,client}.rs`, `crates/magnetar-runtime-moonpool/src/{lib,driver,client}.rs` — `TopicListChange`, `take_topic_list_change`, `poll_topic_list_change`.
- `crates/magnetar/src/engine/{mod,tokio,moonpool}.rs`, `crates/magnetar/src/client.rs`, `crates/magnetar/src/pattern_consumer.rs` — façade surface and `PatternConsumer::update`.
- `crates/magnetar/tests/pattern_watcher_delta_isolation.rs`, `crates/magnetar/tests/pattern_concurrent_update_single_child.rs`.
- `crates/magnetar-differential/tests/facade_pattern_watcher_delta_isolation.rs` — `a_lone_consumer_applies_the_delta_of_its_own_watcher`, `update_does_not_apply_a_delta_addressed_to_another_watcher`, `the_owner_of_a_delta_still_receives_it_after_another_consumer_updated`, on the tokio and the moonpool engine.
- `crates/magnetar-differential/tests/facade_pattern_concurrent_update_single_child.rs` — `overlapping_updates_register_a_duplicate_announcement_once`, on the tokio and the moonpool engine.
- `crates/magnetar-differential/tests/topic_list_watcher_contract.rs` — `a_snapshot_names_the_watcher_its_request_allocated`, `a_snapshot_answering_a_cancelled_watch_records_no_outcome` (it asserted the echoed id before the amendment above).
- [ADR-0037](0037-multi-topics-pattern-consumer-pass-2-lift.md), [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md).
