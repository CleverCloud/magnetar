# ADR-0156 — Close a topic-list watcher when its last user is closed or dropped

- **Status**: Accepted
- **Date**: 2026-11-21
- **Decider**: Florentin Dubois
- **Tags**: pip-145, pattern-consumer, topic-watcher, resource-leak, state-machine, tokio, moonpool, sans-io
- **Amends**: [ADR-0037](0037-multi-topics-pattern-consumer-pass-2-lift.md), [ADR-0136](0136-register-acknowledged-topic-list-watchers-again-after-a-reconnect.md)

## Context

`Connection::watch_topic_list` writes `CommandWatchTopicList`, which makes the broker hold a watcher for the life of the connection and push a `CommandWatchTopicUpdate` for every matching topic change.
Nothing sent `CommandWatchTopicListClose`: the only occurrences of the name outside generated code were two comments (`topic_watcher.rs`, the module doc and the `#[allow(dead_code)]` note on `TopicWatcherRegistry::close` that called the wiring "upcoming").
Three things followed, each on a path the application takes in ordinary use:

1. **A snapshot the caller gave up on.**
   A dropped `topic_list_snapshot` future, or an operation deadline that fires, calls `Connection::cancel_request`, which removed the registry entry for a request still pending.
   The registration is already on the wire, so the broker kept a watcher for it.
2. **An acknowledged watcher whose owner is gone.**
   `PulsarClient::topic_list_snapshot` returns a `Vec<String>` and no handle; `PatternConsumer::close` and the drop path close the children only, and the consumer's `Inner` kept the watcher id only to take its own deltas with (ADR-0132), never to close the watcher.
   The registry entry (`by_watcher`, and `by_request` until the next reset) and the broker's watcher stayed until the connection ended.
   Since ADR-0136 they outlive the session too: every reconnect registers each acknowledged watcher again, and the broker answers each with an update stream.
3. **A watcher the broker refused** kept its (uninitialised) registry entry until the next reset, because the `Error` arm removes the pending kind that `cancel_request` would have used to find it.
   This is memory only; the broker holds nothing.

A fourth path, a delta queued for a watcher the connection does not track, is already closed on `main` by [ADR-0132](0132-address-pip-145-topic-list-deltas-to-their-watcher.md): the `CommandWatchTopicUpdate` arm queues a `TopicListChanged` only for a watcher the registry holds.
That filter is what makes a close stop the queue growing, for the updates already in flight when the broker receives it too; before this change nothing ever removed a watcher from the registry, so it filtered nothing in practice.

Three facts constrain the design and are easy to miss:

- `RequestFut::cancellable` drops through `Connection::cancel_request` in **both** runtimes whether or not the outcome was already taken.
  `cancel_request` therefore runs at the end of every successful snapshot, including the one `PatternConsumerBuilder::subscribe` keeps for its consumer, and cannot be made to close an acknowledged watcher.
- Deltas carry their watcher id and each owner takes only its own ([ADR-0132](0132-address-pip-145-topic-list-deltas-to-their-watcher.md)), so closing a watcher needs no routing change.
- A reply can land before the future awaiting it is dropped, or before its deadline branch is polled.
  The `Success` arm has then removed the pending kind and recorded the outcome, and the drop calls `cancel_request`, which discards the outcome and finds no pending kind.
  The watcher is acknowledged and nobody holds it, and no state-machine entry point can tell that apart from a snapshot whose outcome was taken.

`PulsarClient::inner` is `pub(crate)`, so on the tokio façade an external caller of the public `topic_list_snapshot` cannot poll the deltas its watcher produces.
The moonpool façade does expose a path: `PulsarClient::<MoonpoolEngine<P>>::runtime_client()` returns the runtime client, whose public `poll_topic_list_change` and `next_topic_list_change` read the same client-wide queue, so the documented "live updates are surfaced through `poll_topic_list_change`" was reachable there.
No caller in the tree (tests, examples, docs) paired the façade snapshot with such a read, and the snapshot keeps no watcher id to take the deltas with (`poll_topic_list_change` takes one, ADR-0132); `docs/follow-ups.md` §31 already planned its deregistration once its reply is read.

## Decision

A watcher lives exactly as long as its last user.
The connection closes a watcher it stops tracking, and each runtime ties a watcher to a lease the caller holds from the moment the registration is staged.

- **`magnetar-proto`.**
  `Connection::close_topic_watcher(watcher_id)` removes the watcher from the registry and stages one `CommandWatchTopicListClose` under a fresh request id.
  It is idempotent (an untracked id stages nothing) and registers no pending request: the broker's `Success` and `Error` for an unknown request id are already consumed silently by their arms, so the close records no outcome.
  A watcher that is `Owed` a registration (a reset took its session down and no registration went out on the new one) is only removed, with no frame; `Live` and `Awaiting` watchers also get the frame, and the late reply to an `Awaiting` registration is ignored because the watcher is gone.
  `cancel_request` calls it for a request still pending as a `TopicWatcher`, in place of `TopicWatcherRegistry::close` alone: the registration is on the wire.
  The `Error` arm for a `TopicWatcher` request removes the registry entry without a frame, and still records the error outcome the awaiting future takes.
  `Connection::watch_topic_list_with_watcher_id` returns the watcher id the registration names beside the request id (`watch_topic_list` keeps its signature and delegates), so the id reaches the runtime at registration, under the same lock, rather than with the reply.
  `TopicWatcherRegistry::close` returns where the removed registration stood, which is how the entry point tells the three states apart, and loses its stale `#[allow(dead_code)]`.
- **Deltas.**
  Unchanged by this decision: the `CommandWatchTopicUpdate` arm already queues a `TopicListChanged` only for a watcher the registry holds (ADR-0132) and drops the rest.
  Once a watcher is closed and forgotten, that filter is what stops the queue growing, including for the updates already in flight when the broker receives the close.
  Deltas already queued stay until drained: they are bounded by what arrived before the close, and no cap is added (rejected below).
- **The lease.**
  Each runtime's `Client` gains `watch_topic_list_leased_with_operation_deadline`, which returns the watcher id, the topics and a `TopicWatcherLease { shared, watcher_id }`.
  The lease is created right after the registration is staged and before the first await, and its `Drop` calls `close_topic_watcher` under the connection lock and wakes the driver, like `ConsumerCloseGuard` (ADR-0077).
  Every way out therefore closes the watcher: an error or a deadline (the pending registration was also closed by `cancel_request`, so the lease finds nothing tracked and stages nothing more), a dropped future, and a reply that landed unread (third fact above), which only the lease can close.
  The wake matters: `cancel_request` stages its frame without waking the driver, so without it a cancelled watch's close waits for the next unrelated wake-up.
  A request severed by a reconnect is re-issued under a new watcher id and a new lease (the severed watcher is already gone from the connection, so its lease closes nothing).
  The existing `watch_topic_list_with_operation_deadline` keeps its signature (it resolves to the watcher id and the topics) and its persistent watcher: it delegates to the leased variant and `detach`es the lease on success, so no branch is duplicated and a caller that pairs it with `next_topic_list_change` is unchanged.
  The lease type is public because it appears in a public signature; `detach` is private.
- **The façade.**
  `BrokerMetadataApi` gains one required method, `watch_topic_list_leased_with_deadline`, which returns the watcher id, the topics and a `WatcherLease` (`Box<dyn Debug + Send + Sync>`, an opaque handle whose drop closes the watcher), with no default body: a default no test reaches would be a production line outside the patch-coverage gate, and the only implementors in tree are the two engines' clients.
  The trait is public and not sealed, so an out-of-tree implementor would have to add it.
  `PulsarClient::topic_list_watch_with_deadline` wraps it (the snapshot-only `topic_list_snapshot_with_deadline` it replaces had no other caller), and `PatternConsumerBuilder::subscribe` holds the lease from the snapshot on, before the child-subscribe loop, so a failed child subscribe returns early and closes the watcher too.
  `Inner<C>` stores it as `_watcher`.
  `PatternConsumer::close` on the last handle drops `Inner`, and dropping the last handle does the same, so close and drop are one mechanism.
  A close with clones outstanding stays the recorded no-op: the last clone's drop sends the frame.
  The `Drop` takes the connection lock, and `Inner` is dropped outside any slot mutex, so lock order (global before per-slot) holds.
  The public `topic_list_snapshot` becomes a one-shot: it drops the lease before it returns, and its documentation stops promising a delta stream no external caller can read.
- **Reconnect.**
  A closed watcher is gone from the registry, so `begin_reattach` no longer registers it again: the growth ADR-0136 left (`docs/follow-ups.md` §31, first half) ends.
  Reconciling what changed while the connection was down stays open.

## Alternatives rejected

- **Let `cancel_request` close an acknowledged watcher too, with no new API.**
  `RequestFut::cancellable` runs it after every reply, so the watcher the pattern builder keeps would be closed the moment the snapshot returned.
  Telling a discarded outcome from a taken one inside `cancel_request` (the outcome is still in `outcomes` only when it was never taken) would close the reply-landed-unread case, but it needs a registry lookup by request id, a new branch and a test in every layer, and it still leaves the persistent caller with nothing that says who owns the watcher.
  The lease closes the same case with no extra branch.
- **Close by request id.**
  `begin_session` clears `by_request` and a re-registration takes a new request id, so the id a caller holds names nothing after the first reconnect; the watcher id is the only stable key.
- **Close with the watcher id the reply carries.**
  `OpOutcome::TopicListSnapshot` already carries the watcher id (ADR-0132), but only once the reply is taken: a watch that never gets a reply, or whose reply lands after its future is gone, leaves the caller nothing to close with.
  The id is known at registration, so `watch_topic_list_with_watcher_id` returns it there.
- **An `Arc<dyn Fn(u64)>` closer on the client plus a guard type in the façade.**
  It needs two new `BrokerMetadataApi` methods (the owned watch and the closer), a hand-written `Debug` for the guard (`Inner` derives it) that a test must format, and the lease exists only after the reply, which reopens the reply-landed-unread case.
  The lease needs one method, derives `Debug`, and exists from the registration.
- **Cap `topic_list_changes` and drop the oldest delta.**
  A delta is not idempotent: losing an `added` leaves a matching topic unsubscribed for good, and losing a `removed` leaves a child on a deleted topic.
  A cap is only safe with a re-snapshot, which is the second half of §31 and a larger change.
- **Close only in an explicit `PatternConsumer::close`.**
  Most consumers end by drop, and `close` with a clone outstanding is a no-op by contract, so the leak would remain on the common path.
- **Make `topic_list_snapshot` return a handle.**
  A public signature change for a call whose purpose is "list the topics now", when closing before it returns gives the same result with no new type.
- **Keep the snapshot's watcher and document it.**
  The only reader of its deltas in the tree is a `PatternConsumer`, which would apply them to a pattern they do not belong to; a moonpool-façade caller reading them through `runtime_client()` gets every watcher's deltas mixed in one queue, so the stream it would keep is not one it can attribute to its snapshot.

## Consequences

- A closed or dropped `PatternConsumer`, a finished or abandoned snapshot, and a timed-out snapshot each leave no watcher at the broker, no registry entry, and no further growth of the delta queue.
- One extra frame per watcher at its end; a reconnect costs one `CommandWatchTopicList` per watcher still in use instead of one per watcher ever made.
- `BrokerMetadataApi` gains a required method, which breaks an out-of-tree implementor; `OpOutcome` is unchanged.
- A caller that relied on `topic_list_snapshot` leaving a watcher for `poll_topic_list_change` loses it.
  On the tokio façade no public path reaches that queue; on the moonpool façade `runtime_client()` does, so this is a behaviour change `CHANGELOG.md` records, and such a caller moves to the runtime client's own `watch_topic_list`, which keeps its persistent watcher.
- Drop takes the connection lock.
  A close after a reset only removes the `Owed` entry and stages no frame, and a close on a connection that was shut down only forgets the watcher and stages a frame nothing writes; `topic_watcher_close_lifecycle.rs` pins both.
- A raw runtime-client caller of the persistent `watch_topic_list` whose future is dropped after the reply landed unread is now covered by the lease as well.
  A caller that drives `Connection::watch_topic_list` and `cancel_request` itself, without a lease, still orphans an acknowledged watcher in that window: the state machine cannot distinguish it, and no in-tree caller does this.
- A `PatternConsumer::start_auto_reconcile` task holds a clone for as long as it runs, so `close()` on the caller's handle is the recorded no-op and the watcher closes only when the task ends (every child closed, or the `JoinHandle` aborted) and the last clone drops.
  A pattern that matches no topic never ends the task by itself.
- Not decided here: deltas already queued at the close, and the outage window.
- Still open, and not a leak of a watcher nobody uses: the client-wide delta queue grows for as long as a _live_ pattern consumer's watcher is registered and nothing drains it.
  `PatternConsumer::update` drains it, and `subscribe_with_listener_and_consumer` ([ADR-0140](0140-return-the-pattern-consumer-next-to-its-listener-handle.md)) hands the caller a consumer to call it on, but `subscribe_with_listener` hands its only handle to the poller and its queue grows with the namespace's topic churn for the life of the listener handle.
  Bounding that queue is the capped-queue alternative above and needs a re-snapshot, so it stays with `docs/follow-ups.md` §31.

### ADR-0024 test layers

(a) `magnetar-proto/tests/topic_watcher_close.rs` (a cancelled watch is closed; a delta for a cancelled or unknown watcher queues nothing and a tracked watcher's delta still surfaces, both of which main's filter already guaranteed) and `topic_watcher_close_lifecycle.rs` (an acknowledged watcher is closed once and forgotten, an unknown id stages nothing, an `Owed` watcher is only forgotten and not registered again, an `Awaiting` watcher is closed and its late reply ignored, a refused watch is forgotten without a frame, and a close on a shut-down connection only forgets); plus the in-module `conn_state_tests::cancel_request_removes_the_topic_watcher_of_a_refused_watch` and the registry's `close_reports_where_the_registration_stood_and_nothing_for_an_unknown_watcher`.
(b) `magnetar-runtime-tokio/tests/topic_watcher_close.rs`, `topic_watcher_close_lifecycle.rs`, and `topic_watcher_lease.rs` (a stub broker over TCP: dropping the lease closes exactly its watcher, a persistent watch is kept, a refused watch closes nothing, and a deadline closes the watcher once); with the in-module `client::tests` for the reply-landed-unread case and `detach`.
(c) the moonpool twins of those three files and of the in-module tests.
(d) `magnetar-differential/tests/topic_watcher_close_equivalence.rs` and `topic_watcher_close_lifecycle_equivalence.rs` (connection level, both engines) `pattern_watcher_close_equivalence.rs` (façade level against an in-process fake broker, both engines: `topic_list_snapshot`, `PatternConsumer::close` and dropping the last handle each close the one registered watcher, and a live consumer's watcher stays registered; two controls pass today: a raw peer's close is recorded by the fake, and the live consumer's watcher is not closed) and `pattern_watcher_build_failure_equivalence.rs` (the fake returns a matching topic and refuses its lookup, so the build fails after the watcher was registered, and the watcher is closed with the early return).
No end-to-end test is added by this changeset. One is possible with the frame proxy of the end-to-end suite (`crates/magnetar/tests/common/frame_proxy.rs`): build a `PatternConsumer` through the proxy, call `drop_next_client_frame` for `WatchTopicListClose`, close or drop the consumer, and assert that `dropped()` counts one frame; it is not written here. A case that only asserts that no update reaches the client after the close cannot fail on this base, because the `CommandWatchTopicUpdate` arm already drops an update for a watcher the registry no longer holds (ADR-0132). The existing `e2e_pattern_auto_reconcile.rs` builds a pattern consumer against Pulsar, discovers a topic created afterwards through the consumer's own watcher and closes the consumer, so it shows that the watcher stays registered while its user lives; it does not observe the close frame.

## References

- `crates/magnetar-proto/src/conn.rs` — `watch_topic_list_with_watcher_id`, `close_topic_watcher`, `cancel_request`, the `WatchTopicUpdate` and `Error` arms.
- `crates/magnetar-proto/src/topic_watcher.rs` — `TopicWatcherRegistry::close`, `begin_session`, `begin_reattach`.
- `crates/magnetar-runtime-tokio/src/client.rs`, `crates/magnetar-runtime-moonpool/src/client.rs` — `TopicWatcherLease`, `watch_topic_list_leased_with_operation_deadline`, `RequestFut::cancellable` and its `Drop`.
- `crates/magnetar/src/engine/mod.rs`, `crates/magnetar/src/pattern_consumer.rs`, `crates/magnetar/src/client.rs` — `BrokerMetadataApi::watch_topic_list_leased_with_deadline`, `PatternConsumer::close`, `Inner`, `topic_list_snapshot`.
- [ADR-0136](0136-register-acknowledged-topic-list-watchers-again-after-a-reconnect.md) — re-registration across a reconnect; `docs/follow-ups.md` §31.
- [ADR-0077](0077-consumer-last-clone-drop-close.md) — the last-clone drop-guard close this reuses.
