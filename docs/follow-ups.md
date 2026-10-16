# Open Follow-Ups

Consolidated tracker for known open work.
Each entry lists the gap, the reason it stays open, and (where actionable) a `/goal …` block ready to be copy-pasted verbatim into a fresh session for an agent team to pick up.

For the public-facing parity status, see the [parity matrix in the README](../README.md#java-client-parity-matrix).

This file is the **single source of truth** for what is intentionally deferred or blocked.
Anything not listed below is either already shipped (check `git log` for the implementation reference) or explicitly out of scope ([ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) §D-series, [ADR-0120](../specs/adr/0120-assignment-driven-m1-hardened-stream-consumer.md), [ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)).

When a PR closes an item, the entry is **removed** (git log + the ADR / docs file carry the post-implementation reference); partially-closed items are trimmed to their remaining open residual.

**API stability stance.** The crates are published (`magnetar-driver`, `magnetar-proto`, and the rest of the workspace).
Breaking API changes are still acceptable when they improve correctness, ergonomics, or layering, but each one must carry a `BREAKING CHANGE:` footer in the commit body, a `CHANGELOG.md` entry, and an explicit statement of whether the ergonomic façade surface is affected or only the low-level `magnetar-proto` API (re-exported as `magnetar::proto`).
See [ADR-0086](../specs/adr/0086-inject-now-into-proto-latency-recording.md) for a worked example.

---

## Index

Status tags: ⚡ ready to dispatch · 🔗 blocked on external dep · ⏳ blocked on upstream PIP release · 🧠 needs design decision · 🔬 needs reproduction · 🟡 deferred (not load-bearing).

| #   | Item                                                                                                                                                     | Status                   |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| 12  | [PIP-460 remaining GA wire and API gaps](#12-pip-460-remaining-ga-wire-and-api-gaps)                                                                         | ⚡ ready to dispatch     |
| 16  | [The batched `deliver` loop counts dead-lettered members as delivered](#16-the-batched-deliver-loop-counts-dead-lettered-members-as-delivered)           | 🟡 deferred              |
| 17  | [The PIP-33 marker branch returns before `maybe_flow`](#17-the-pip-33-marker-branch-returns-before-maybe_flow)                                           | 🟡 deferred              |
| 18  | [`dead_letter_pending` is unbounded and never auto-drained](#18-dead_letter_pending-is-unbounded-and-never-auto-drained)                                 | 🧠 needs design decision |
| 19  | [Re-home an established producer or consumer to another broker connection](#19-re-home-an-established-producer-or-consumer-to-another-broker-connection) | 🧠 needs design decision |
| 20  | [No deadline on a pending `ProducerOpen` / `Subscribe` request](#20-no-deadline-on-a-pending-produceropen--subscribe-request)                            | ⚡ ready to dispatch     |
| 21  | [Optional partitioned-router readiness skip](#21-optional-partitioned-router-readiness-skip)                                                             | 🧠 needs design decision |
| 22  | [Memory reservations of publishes the broker will never answer](#22-memory-reservations-of-publishes-the-broker-will-never-answer)                       | 🟡 deferred              |
| 23  | [Dropped send futures leave their outcome behind](#23-dropped-send-futures-leave-their-outcome-behind)                                                   | ⚡ ready to dispatch     |
| 24  | [A Reader started inside a batch re-delivers the members before it](#24-a-reader-started-inside-a-batch-re-delivers-the-members-before-it)               | ⚡ ready to dispatch     |
| 25  | [`permit_balance` cannot go negative](#25-permit_balance-cannot-go-negative)                                                                             | 🟡 deferred              |
| 26  | [`receive_batch` inner pops do not wake the driver](#26-receive_batch-inner-pops-do-not-wake-the-driver)                                                 | ⚡ ready to dispatch     |
| 27  | [An undecodable batched entry is dropped without an ack](#27-an-undecodable-batched-entry-is-dropped-without-an-ack)                                     | 🧠 needs design decision |
| 28  | [Batched + encrypted messages are not Java-compatible](#28-batched--encrypted-messages-are-not-java-compatible)                                          | 🧠 needs design decision |
| 29  | [Intermittent high ACK/NACK reconnect test](#29-intermittent-high-ack-nack-reconnect-test)                                                              | 🔬 needs reproduction    |

---

## 12. PIP-460 remaining GA wire and API gaps

**Status.** Assignment-driven `StreamConsumer` fan-out, ordinary segment children, aggregate flow control, delivery tracking, acknowledgements, transactions, seek, resynchronization, type-82 unsubscribe, and PIP-486 consumer bucket dispatch are implemented by [ADR-0120](../specs/adr/0120-assignment-driven-m1-hardened-stream-consumer.md). Scalable-topic producers, `QueueConsumer`, `CheckpointConsumer`, namespace consumers, controller-election awareness, and repartition handling remain outside this implementation; the feature stays experimental and default-off.

- **Scalable-topic producer entry bucketing is not implemented.** A 5.0.0 topic may carry `entryBucketSplits=[16384,32768,49152]` (measured 2026-10-12). `MessageMetadata.entry_hash_min` / `entry_hash_max` and `SegmentInfoProto.entry_bucket_splits` define the producer-side contract; no scalable producer currently stamps and honours it. Consumer assignment ranges are carried separately in `ScalableAssignedSegment.bucket_ranges` and consumed by `StreamConsumer`.
- **A namespace (multi-topic) consumer passes `create_if_missing = false`** to `open_scalable_topic_session` on its per-topic lookups, as the Java v5 client does (`MultiTopicQueueConsumer.java:169` at `v5.0.0`), so a reconnecting watch cannot resurrect a deleted topic. The namespace consumer surface does not exist yet.

## 16. The batched `deliver` loop counts dead-lettered members as delivered

**Gap.** `ConsumerState::deliver`'s batched branch increments its own `delivered` counter after every `classify_and_queue` call, including for a member the dead-letter branch routed to `dead_letter_pending` rather than to the queue.
The branch then returns `DeliverOutcome::Delivered { count: delivered }`, so `count` over-reports by the number of dead-lettered members.
`conn.rs`'s `Message` arm reads `count` as "the number of newly delivered tail entries" and clones `queue[queue_len - count ..]` to emit one `ConnectionEvent::Message` per entry, so an over-reported count makes it re-emit observational events for OLDER queued entries it already announced.

**Why it stays open.** It is adjacent to [ADR-0107](../specs/adr/0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md) and read-verified, but it changes event emission rather than flow accounting, so it wants its own test layers and its own line in the compatibility story — an application counting `ConnectionEvent::Message` sees a behaviour change.
The permit ledger is unaffected either way: `record_dispatch_unit` and the dead-letter refund are per-member and do not read `delivered`.

## 17. The PIP-33 marker branch returns before `maybe_flow`

**Gap.** `conn.rs`'s `Message` arm filters a replicated-subscription marker, calls `consumer.record_marker_consumed()` — which credits `consumed_since_flow` — and returns from the arm before reaching the `consumer.maybe_flow()` call the ordinary delivery path makes.
A marker-only stream therefore accrues refunds it never emits: the grant waits for the next non-marker frame, or for a `pop_message` that a stream carrying no user messages never gets.

**Why it stays open.** The accounting is correct (the ledger holds the credit, nothing is lost) and the practical window is a replicated subscription with no user traffic at all, so the cost is latency to the next grant rather than a wedge.
Closing it means deciding whether the marker path should emit its own flow or whether the keepalive sweep should drain the ledger, which is a small design call and the ADR-0024 five layers.

## 18. `dead_letter_pending` is unbounded and never auto-drained

**Gap.** `ConsumerState::dead_letter_pending` is a plain `Vec<IncomingMessage>` with no cap, and every caller that empties it is user-driven (`Consumer::drain_dead_letter`, `Consumer::republish_dead_letters` and the aggregate wrappers).
Before [ADR-0107](../specs/adr/0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md) the missing flow refund was an accidental bound: the subscription wedged once a receiver queue's worth of poison had accumulated, so the buffer stopped growing.
With the refund in place a poison-heavy topic keeps dispatching and the buffer grows until the application drains it.

**Why it stays open.** Needs a product decision, and the Java client offers no precedent: it has no client-side dead-letter buffer at all, it republishes to the DLQ topic and acks inside `messageReceived`.
The options are not equivalent — cap and drop (loses messages the application asked to see), cap and stop refunding (reintroduces the wedge deliberately, with a documented contract), or auto-republish when a producer is configured (changes what `dead_letter_policy` means).
There is a residual bound today regardless: a dead-lettered unit stays unacked at the broker until `republish_dead_letters` acks it, so an application that never drains eventually stops at `maxUnackedMessagesPerConsumer`.

## 19. Re-home an established producer or consumer to another broker connection

**Gap.** [ADR-0106](../specs/adr/0106-reattach-broker-closed-producer-in-place.md) re-attaches a broker-closed producer on the connection it is already pinned to, and issue #307 does the same for a consumer.
Neither can follow the bundle to a different broker.
When the `CommandCloseProducer` / `CommandCloseConsumer` carries an `assigned_broker_service_url` naming another broker, or when the retry leg's lookup answers `Redirected`, the in-place re-attach fails: `lookup_then` terminalizes the handle and the application must re-create it.
Java re-homes instead — `ConnectionHandler.grabCnx(hostUrl)` dials the assigned host, or performs a fresh lookup and takes whatever connection it resolves to.

**Why it stays open.** No proto or pool primitive exists for it.
A `Producer` / `Consumer` holds one `Arc<ConnectionShared>` for its lifetime (the engines' `open_producer` / `subscribe` capture it at creation), so re-homing means moving a live handle between two `Connection` state machines — including its pending publishes, its permit mirrors and its registered wakers.
That is an architectural decision with its own ADR, not an amendment to ADR-0106.
The current behaviour is a bounded terminal error rather than a silent hang, which is the part that mattered for issue #451.

The consumer arm's own `assigned_broker_service_url = Some(url)` branch belongs to the same decision: it still diverts to the supervised reconnect, and ADR-0106 deliberately did not transfer the producer-side argument to it without separate evidence.

## 20. No deadline on a pending `ProducerOpen` / `Subscribe` request

**Gap.** `Connection::handle_timeout` sweeps only `PendingRequestKind::Ack`.
A `CommandProducer` or `CommandSubscribe` the broker never answers leaves the slot at `broker_ready = false` (or the consumer's flow gate armed) with `open_request_id = Some(_)` forever.
That reproduces the issue #451 symptom exactly — every publish resolves `code=-1 send timeout` — and it additionally makes every LATER broker close ineligible for an in-place re-attach, because `open_request_id.is_some()` is a refusal.

**Why it stays open.** It is shared by every re-attach path — the ADR-0080 retry leg, the reconnect rebuilds, the issue #307 consumer re-subscribe and ADR-0106's producer re-attach — so it belongs to the request-deadline surface as a whole, not to any one of them.
Java covers it with `operationTimeout` applied to every pending request; the equivalent here is a per-kind deadline on `pending_requests` plus the terminalization each kind already has.

## 22. Memory reservations of publishes the broker will never answer

**Gap.** Since [ADR-0111](../specs/adr/0111-share-one-memory-limit-controller-per-client.md) a publish holds its bytes against the client-wide `memory_limit` until its op leaves the pending queue, so an op the broker will never answer holds budget for EVERY producer of the client until something fails it.
Two known cases: batched sends already flushed but unacknowledged when the broker closes the producer and it re-attaches in place ([ADR-0106](../specs/adr/0106-reattach-broker-closed-producer-in-place.md)) — their per-message ops carry empty `replay_frames`, so `ProducerState::replay_pending_outbound` never re-sends them — and the sends of a producer whose `CommandCloseProducer` the broker rejected.
The default 30 s `send_timeout` fails and releases them; with `send_timeout: None` they are held for the life of the client.

**Why it stays open.** The ops themselves predate ADR-0111 (their futures already hung or timed out); only the budget they hold is new.
Closing it means failing un-replayable batched ops on an in-place re-attach the way `Connection::reset` already does on a reconnect, and deciding what a rejected close leaves pending — each with the ADR-0024 five layers.

## 23. Dropped send futures leave their outcome behind

**Gap.** When an op resolves — receipt, send error, send timeout, terminal failure, and since ADR-0111 the acknowledgement of its producer's close — `Connection` records an `OpOutcome` under its `PendingOpKey::Send` unconditionally.
If the caller already dropped its `SendFut`, nothing ever takes that entry, so every fire-and-forget send leaves one permanent `outcomes` entry: the issue #241 leak shape, which the request paths already guard with "record only while a waker is parked".

**Why it stays open.** It predates ADR-0111 on the receipt path, and the fix crosses both engines: the `Send` arms need the same waker-presence guard, and a `SendFut` dropped between the wake and its next poll must still find its outcome, so the guard and the future's `Drop` have to agree on who removes the entry.

## 24. A Reader started inside a batch re-delivers the members before it

**Gap.** A Reader opened at a start message id that points inside a batched entry receives the whole entry, members `0..k-1` included.
Java skips those members and refunds their permits (`ConsumerImpl.java` around `:1318-1324` and `:1874-1875`).

**Why it stays open.** Recorded by [ADR-0112](../specs/adr/0112-compress-and-decompress-a-batch-as-one-body.md) as a follow-up; it is a Reader semantics change with its own test layers.

## 25. `permit_balance` cannot go negative

**Gap.** `ConsumerState::permit_balance` is a `u32` decremented with `saturating_sub`, so the negative balance a broker reaches after a forced whole-entry dispatch reads as `0` on the client.

**Why it stays open.** The refund ledger, not the balance, drives `maybe_flow`, so this only blurs the starvation signal; widening the field is an API change on a public field.

## 26. `receive_batch` inner pops do not wake the driver

**Gap.** The tokio `Consumer::receive_batch_with_bytes_cap` pops every message after the first under the connection lock without calling `driver_waker.notify_one()`, so a `CommandFlow` one of those pops queues waits for the driver's next wake instead of leaving at once.

**Why it stays open.** Latency rather than a wedge — any later wake flushes it — and found while fixing issue #860, outside its scope.

## 27. An undecodable batched entry is dropped without an ack

**Gap.** Since [ADR-0112](../specs/adr/0112-compress-and-decompress-a-batch-as-one-body.md) a batched entry no layout can decode is refunded and logged, but never acknowledged, so a durable subscription's mark-delete position stays behind it.
Java's `discardCorruptedMessage` acknowledges such an entry with a validation error.

**Why it stays open.** Acknowledging deletes data a fixed client could still read, so whether to follow Java is a product decision.

## 28. Batched + encrypted messages are not Java-compatible

**Gap.** Producer side: Java encrypts the whole compressed batch body once and stamps the keys on the batch metadata; `ProducerState::flush_batch` builds the batch's `MessageMetadata` fresh, so since [ADR-0112](../specs/adr/0112-compress-and-decompress-a-batch-as-one-body.md) an encrypted message is never batched at all — an encrypting producer with batching enabled sends one entry per message.
Consumer side: a Java batched + encrypted entry has to be decrypted as one body, then decompressed, then split; `ConsumerState::deliver` cannot decrypt — the decryptor lives in the engine, after the pop — so such an entry is undecodable and is dropped and refunded with one `warn!`.

**Why it stays open.** Both halves are a wire change of their own: the encryptor has to run over the batch body inside `flush_batch`, and the decryptor has to reach `deliver` (or the split has to move after the engine's decrypt step).

## 29. Intermittent high ACK/NACK reconnect test

**Gap.** `e2e_unconfirmed_high_ack_never_skips_nacked_lower_message` is temporarily ignored while its intermittent reconnect result is investigated; its assertions remain intact. The exception and reactivation criteria are defined in [ADR-0046](../specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md).

**Why it stays open.** The proposed upstream cursor-cleanup race itself has not been reproduced at runtime, and its causal link to this intermittent result is unproven.

## 30. Exact intermittent test quarantine scenarios

**Gap.** Three exact scenarios have temporary, non-gating test-result diagnostics while their intermittent results are investigated. Names, source evidence, owner, the 2026-10-30 review date, and reactivation conditions are recorded in the [test-quarantine register](test-quarantine.md) under [ADR-0123](../specs/adr/0123-quarantine-exact-intermittent-test-scenarios.md). The separate ACK/NACK exception remains item 29.

**Why it stays open.** Test assertions remain intact, and the focused diagnostic runs provide evidence for deciding when each exact scenario can return to the blocking suite.

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

§11 closed with [ADR-0120](../specs/adr/0120-assignment-driven-m1-hardened-stream-consumer.md), which exposed the owned façade constructor on both engines. §1 closed with [ADR-0093](../specs/adr/0093-pip-460-upstream-wire-surface.md), which migrated PIP-460 onto the upstream wire surface and added real-broker e2e coverage; §8 closed with [ADR-0091](../specs/adr/0091-broker-authority-default-port-unification.md), §10 with [ADR-0092](../specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md), §12 was partially closed by [ADR-0120](../specs/adr/0120-assignment-driven-m1-hardened-stream-consumer.md), §14 closed with [ADR-0118](../specs/adr/0118-isolate-sim-coverage-current-pass-artifacts.md) and [ADR-0120](../specs/adr/0120-assignment-driven-m1-hardened-stream-consumer.md), and §15 closed with [ADR-0119](../specs/adr/0119-use-tokio-time-for-driver-write-deadlines.md). §13 closed with `e93deee`, which woke the scalable waiters on disconnect in both engines.
Numbering is stable, not contiguous: closed items are removed and their number is retired rather than reused, so a `§N` reference in a commit, ADR, or code comment keeps pointing at the same item forever.
