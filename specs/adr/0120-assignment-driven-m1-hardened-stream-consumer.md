# ADR-0120 — Adopt the assignment-driven M1-hardened scalable StreamConsumer

- **Status**: Accepted
- **Date**: 2026-08-05
- **Decider**: Florentin Dubois
- **Tags**: pip-460, scalable-topics, stream-consumer, assignment, ordering, flow-control, transactions, coverage
- **Supersedes**: The drop-on-DAG-change and unimplemented-data-plane scope decisions that survived [ADR-0031](0031-pip-460-scalable-subscription-scope.md) and were repeated in [ADR-0093](0093-pip-460-upstream-wire-surface.md), plus ADR-0093 § D5's blanket rejection of non-advancing consumer assignments and [ADR-0095](0095-ignore-a-re-sent-scalable-layout-epoch.md)'s preserved assignment-side contrast
- **Preserves**: ADR-0093's vendored Pulsar 5.0.0 GA wire surface and per-connection capability negotiation, plus [ADR-0095](0095-ignore-a-re-sent-scalable-layout-epoch.md)'s duplicate-layout handling
- **Amends**: The compiled coverage closure and record-less-file treatment recorded by [ADR-0090](0090-widen-sim-coverage-report-to-compiled-closure.md) and retained by [ADR-0118](0118-isolate-sim-coverage-current-pass-artifacts.md)

## Context

ADR-0093 moved PIP-460 from a projected protocol onto the wire surface Apache Pulsar 5.0.0-M1 actually ships, but deliberately left the high-level `StreamConsumer` as a layout observer.
The surviving scope dropped that observer on a DAG change, and no ordinary child consumer attached to the `segment://` topics granted by `ConsumerAssignment`.
The façade constructor was also unusable because it required `E::ClientState: Clone` while neither public runtime client implements `Clone`.

The M1 Java client establishes the missing data-plane shape: controller registration yields whole-segment assignments, and each assigned segment is consumed through an ordinary `Exclusive` child under one subscription.
That shape alone does not settle ordering, lifecycle, or fencing.
M1 carries no assignment generation or controller term, no scalable-consumer unregister command, no remote ancestor-completion state, and no defined proxy-any-broker route for leader-only registration.
Magnetar therefore needs local safety contracts that are explicit about what they prove and what remains an upstream gap.

The implementation also changes the sim-coverage closure.
`magnetar-differential` now has dev-dependencies on the published façade package `magnetar-driver` at directory `crates/magnetar` and on `magnetar-fakes`, and its public aggregate tests execute both libraries.
The coverage execution roots remain `magnetar-runtime-moonpool` and `magnetar-differential`; only the set re-exported and hard-gated by the report grows.

## Decision

### Owned generic surface

`PulsarClient::scalable_stream_consumer(topic, Arc<S>)` returns an owned `StreamConsumerBuilder<'_, S, E>`.
The builder requires a subscription, accepts a stable consumer name, defaults to `OrderingMode::Strict`, validates one aggregate `ReceiverBudget`, and creates only ordinary `Exclusive` segment children.
The wire `consumer_id`, controller incarnation, child identifiers, and runtime tasks remain client-owned.

The public Tokio and Moonpool clients remain non-`Clone`.
Each runtime instead implements the internal owned `SegmentSubscriber` capability needed to route the controller, open and operate ordinary children, allocate identifiers, and retain provider-native task and time facilities.
`StreamConsumer<S, E>` is cheap to clone over its own `Arc`-backed aggregate state; `StreamMessage<S>` owns `S::Owned`, and neither `S`, `S::Owned`, nor a runtime client needs to implement `Clone`.

### Assignment and controller authority

The controller assignment grants ownership; the retained DAG watch supplies topology and ordering dependencies.
The high-level consumer keeps both.
It reconciles changed equal-epoch assignments because `layout_epoch` versions topology rather than group membership.

High-level routes are keyed by event family, session or consumer id, and a local monotonically increasing controller-connection incarnation.
The route is installed before registration is written; pushes received before the subscribe response are buffered; the validated response becomes the incarnation baseline; and buffered then later pushes apply in wire order.
Exact duplicates are discarded, lower epochs fail closed, and callbacks, opens, closes, acknowledgements, and reconnect results from an old incarnation cannot mutate current state.
The low-level global scalable event API only receives events that no owned route claimed, so two aggregate consumers cannot steal or duplicate each other's events.
An unclaimed `ConsumerRejected` retires its exact local registration and schedules logical unsubscribe while preserving the rejection diagnostic in the raw event queue. A cancelled raw registration that succeeds still exposes `ConsumerAssigned`; claimed aggregate retry routes retain their terminal cleanup ownership.
Route retirement removes live ownership immediately and retains at most 256 logical tombstones, compacted to the newest consumer incarnation, so recent late events remain fenced without allowing abandoned route identities to grow memory without bound.
Connection replacement and route overflow request resynchronization, while physical connection closure, explicit route closure, and peer closure fail the aggregate terminally instead of entering the reconnect loop again.
Resynchronization marks reconnect intent before child teardown, suppresses expected close errors from obsolete child loops, and waits for confirmed children plus in-flight child opens to finish before registering a replacement controller baseline.
Resynchronization reattaches the same scalable member identity without a logical unsubscribe, then retires the obsolete local registration and route after replacement succeeds. Logical unsubscribe belongs to terminal close or drop; it cannot race an asynchronous deletion against the replacement registration.
A broker rejection during replacement registration is retried on provider time until registration succeeds or the aggregate closes; the physical-connection grace fallback describes historical M1 behavior.

Controller and segment authorities are taken from broker-authored direct URLs matching the bootstrap transport: plaintext uses plaintext authority and TLS uses TLS authority.
Every published authority passes the existing redirect allow-list before credentials are reused.
Pulsar 5.0.0-M1 can omit the controller URL before leader election publishes one; only in that case, and only for a direct bootstrap, controller registration reuses the already-authenticated configured service connection, matching the official V5 client.
Missing segment authority, an explicit transport mismatch or rejected authority, and proxy-any-broker controller registration still return typed routing failures rather than guessing another target or downgrading TLS.

### Complete DAG and ordering contract

Every layout is validated atomically before it can influence delivery.
Validation covers unique segment and placement identities, matching placement sets, valid lifecycle epochs and states, dangling and conflicting edges, reciprocal parent/child edges, acyclicity, bounded nodes/edges/depth/serialized size, canonical assignment attachment identity, split containment and exact coverage, merge union coverage, and non-overlapping active leaves covering the configured key space.

M1 hash ranges are inclusive `[start, end]` within `0..=65535`.
No half-open `65536` sentinel is part of the public or internal contract.

`OrderingMode::Strict` is the default.
It grants descendant FLOW only when retained local ownership history proves every transitive ancestor complete; unknown or cross-member ancestry becomes observable `OrderingUnprovable` state and blocks the affected descendant.
`OrderingMode::BrokerManaged` is an explicit compatibility choice: it applies every locally provable barrier but relies on the broker for ancestry owned by another member and promises no cross-member parent-before-child order.
Neither mode defines a total order between independent branches; ordinary FIFO remains per segment.

A local ancestor completes only after terminal/end-of-topic, every delivered message and pre-terminal reservation is resolved, every required acknowledgement settles, and every participating transaction receives a confirmed commit outcome.
Layout visibility, assignment arrival, seal state, timeout, and queue emptiness are not completion proofs.

### Aggregate receive budget and concurrency

One aggregate budget covers Magnetar-controlled receive storage across all active and retiring children, including granted but unconsumed permits, retained encoded/decompressed payload, chunk and batch work, queue and ledger nodes, canonical-id/source sidecars, position-map nodes, ordering barriers, delivery leases, and retained source-qualified authority.
Selected batch members copy into right-sized payload buffers instead of each retaining the full batch backing allocation.
Both runtimes reserve output plus one validation byte and codec workspace before decrypting and bounded-decompressing inbound broker payloads; zlib reads borrowed input into a bounded destination and zstd caps both destination and window, while Moonpool's producer-side refusal of non-`None` compression remains unchanged.
Every child uses manual FLOW; initial, automatic, reconnect, and refill FLOW are disabled, and only the aggregate arbiter grants a permit after reserving one `MAX_FRAME_SIZE`.
Adding segments redistributes the budget and never multiplies it.
For a partial broker batch, only the logical members selected by `CommandMessage.ack_set` consume aggregate permits.
Fresh grants are retained as replayable credit, while batch overshoot is wire-only debt: it is repaid on the current child but never replayed after seek/reconnect, and seek clears obsolete granted/balance state before new FLOW is admitted.

The exact minimum is:

```text
MAX_FRAME_SIZE
+ 5 * MAX_STREAM_POSITION_SIZE
+ 3 * MAX_POSITION_COMPONENTS * POSITION_COMPONENT_NODE_OVERHEAD
+ 2 * DELIVERY_AUTHORITY_OVERHEAD
+ DELIVERY_ACK_GENERATION_OVERHEAD
+ 64 KiB control-plane cleanup reserve
= 13,697,160 bytes
```

`DELIVERY_ACK_GENERATION_OVERHEAD` is one `u64` charged once with each live delivery lease; the canonical delivered-position map retains its existing charge.

The default and documentation examples use 16 MiB.
The bound covers allocations whose size Magnetar controls or derives from the wire; arbitrary additional memory allocated inside user-defined `S::Owned` during decoding is outside it.

`receive(&self)` and `receive_batch(&self, policy)` support concurrent callers without channels.
Reservation/dequeue order is linearized under aggregate state, each message is reserved once, and future completion order and per-waiter FIFO are unspecified.
After its first-message wait, a batch reserves and removes its complete bounded set atomically, so another receive cannot interleave inside that batch.
Cancellation before reservation consumes nothing; cancellation after reservation either returns an owned delivery synchronously or restores the same live authority at its original dequeue sequence.
If concurrent fencing prevents restoration, the aggregate requests resynchronization rather than silently discarding the delivery.

### Source-qualified delivery, acknowledgement, transactions, and seek

Every delivery exposes a canonical `SegmentSource`, source-qualified `StreamMessageId`, delivered `PositionVector`, and process-local `DeliveryToken`.
The token binds consumer instance, controller and child incarnations, source, ordinary message id, delivery epoch, and dequeue sequence.
Foreign, stale seek/rebalance-generation, and retired-source authority is rejected before wire I/O unless the old source is deliberately retained for drain.

`StreamMessageId` and `PositionVector` use Magnetar's strict version-1 `MSTR` binary envelope.
`DeliveryToken` is not serializable, and a deserialized position restores a value rather than acknowledgement authority.
`DeliveryToken::stream_message_id()` still returns `&StreamMessageId`; it is non-const because tokens share the canonical ID through `Arc`.
An ordinary `MessageId` has no scalable segment field; scalable identity exists in the canonical source plus `StreamMessageId` or `PositionVector`.
The canonical ordinary `MessageIdData` component is capped at 65,536 bytes during both in-memory construction and byte decoding, so an oversized `ack_set` or chunk pointer cannot enter an otherwise bounded envelope.

The aggregate supports individual ack, batch ack, cumulative vector ack, restored-vector ack, negative ack, and individual or vector acknowledgement inside a Pulsar transaction.
A vector fans out with ordinary per-segment semantics; partial success reports confirmed and failed components, and retries are idempotent for confirmed components.
The same cumulative token retries all unconfirmed sources in its saved vector even after its own source is confirmed, without regaining receive or individual-acknowledgement authority.
When an ordinary child receives a partial batch, expansion exposes only `ack_set`-selected members and retains the same effective mask in every source-qualified canonical id.
Individual and cumulative acknowledgements seed from that mask; a cumulative acknowledgement inside the entry clears only selected positions through its index and preserves later selected positions as the residual `ack_set`.

Transactional acknowledgement single-flights registration of every represented `(segment topic, subscription)` and admits every component before commit may close admission.
Commit waits for all admitted operations.
Any admitted registration or acknowledgement failure permanently poisons commit, returns `TransactionPoisoned`, and emits no `EndTxn(Commit)`; abort remains available.
Confirmed commit advances participating positions, abort leaves cursors unchanged and permits redelivery, and an unknown outcome fails participants closed and requires resynchronization.
Only one caller may await `EndTxn` for a transaction at a time; cancellation releases that waiter lease while preserving the canonical request so a same-action retry resumes it without emitting a second wire command.
Once the transaction coordinator confirms commit or abort, an owned runtime completion task records that terminal broker state before notifying local participants, checkpoints each successfully completed participant action, and survives caller cancellation so a retry resumes only unfinished local propagation without replaying FLOW or reissuing `EndTxn`.

Vector seek is limited to the current layout epoch and exactly the currently owned, attached active leaves.
Every eligible source must be represented.
Seek is rejected while receive, batch, delivery, or transactional-ack reservations are active; it cannot cross a layout transition, rewind sealed or remote ancestry, or infer omitted positions.
A canonical seek position is projected onto the fields M1 actually applies: chunked deliveries seek to `first_chunk_message_id`, batched deliveries encode the inclusive suffix as `ack_set`, and `batch_index`, `batch_size`, and nested chunk metadata are omitted from `CommandSeek`.
Both runtimes synchronously stage every eligible child SEEK before awaiting any child response, preventing map iteration or one delayed child from suppressing another current leaf's command.
A successful seek increments the delivery epoch, clears pre-seek buffers, and invalidates old tokens; a partial child failure leaves the aggregate failed and resynchronization-required.
The failed seek publishes reconnect intent before awaiting confirmation-bearing child closes, so an old child cannot race teardown into a second resynchronization or replacement open.

### Ownership transfer and close

Assignment loss stops new FLOW and receive reservations but retains already reserved or delivered messages and the old child as an acknowledgement target.
The drain is unbounded in time, not in memory: it may wait forever for the application to resolve a delivery while remaining inside the aggregate budget.
Only then does the old `Exclusive` child close and allow a replacement generation to open.
An assigned active parent becoming sealed without replacement placement is not ownership loss: the existing connected child and generation drain in place until terminal and retained work settle.
M1 deliberately emits that shape, so reopening the parent would be unrouteable; every other routeable descriptor change still uses the confirmation-bearing replacement fence.
If the retained connection fails before completion, ordinary child-failure resynchronization applies and no sealed authority is synthesized.
A locally completed sealed segment may remain or reappear in a complete assignment after rebalance, but that assignment does not reopen it or create pending ownership; other gained active segments reconcile normally.

A gained segment remains observable as `PendingOwnership` while another process retains its old `Exclusive` child.
Each open attempt is bounded by the ordinary operation deadline, while `ConsumerBusy` schedules another provider-timed attempt for as long as the assignment remains current.
Assignment removal, incarnation replacement, aggregate close, or permanent failure fences that loop.

Explicit close through any clone is globally definitive locally and awaits route, task, and child cleanup.
Cleanup runs in an owned task using the existing aggregate task registry, so cancelling the initiating close future cannot abandon child confirmations or strand concurrent close callers. Each caller waits for the same completion notification; the initiating caller retains its original structured error result.
A transactional acknowledgement or registration that races aggregate close resolves as closed after close-owned model cleanup, rather than exposing an internal invalid-phase failure.
A late successful child open that has already lost its assignment is still closed physically, but its completion does not mutate the retired or replacement model child. Physical close failures remain observable.
Final drop is synchronous best-effort fencing and does not block or spawn.
GA close requests logical scalable unsubscribe after any in-flight registration responses settle; it does not await the unsubscribe's `CommandSuccess`.
The broker's physical-connection grace period remains a fallback when explicit unregister fails.

### Fake, differential, e2e, and coverage evidence

GA same-name registration reattaches a settled membership without rebalance, including transfer to a new consumer id. A delayed unsubscribe retains the captured topic, subscription, consumer name and consumer id. Completion compares that identity against the current membership independently of its physical connection: same-id reattachment is deleted, while a different consumer id preserves the transferred membership. This fake models unsubscribe received before reattachment; it does not retain the old connection's registration reference for a new unsubscribe sent after transfer. The held-delete regression exercises the former ordering, while native protocol tests verify terminal cleanup separately.

`magnetar-fakes` supplies a stateful generated-wire multi-endpoint cluster with controller and segment routes, assignments and equal-epoch rebalances, reconnect baselines, ordinary subscribe/FLOW/message/ack/seek/close behavior, delayed and failed operations, transaction state, drain eligibility, and resource counters.
It validates commands and independent invariants rather than repeating the client state machine.
Ordinary `CloseConsumer` cancels a matching pending Subscribe and releases only its physical reservation; an unknown connection-local consumer id returns correlated Success without changing another connection’s consumer, and late cancelled completion cannot retire a replacement. The fake cancels pending creation atomically; its fresh-id replacement regression does not model GA’s failed-future tombstone that blocks reuse of the same id until asynchronous creation finishes.
Delayed ordinary `CloseConsumer` confirmations, admitted ACK completion, and staged transaction ACK commit or abort are fenced by the child connection, identity, incarnation, and generation independently of controller membership. Ordinary FLOW credit targets the live child on its physical connection independently of controller membership or assignment withdrawal; closed and disconnected children still reject it. Ordinary Subscribe admission and delayed completion use the physical topic placement, connection and consumer id, exclusive subscription owner, pending reservation and generation independently of logical assignment or consumer name. The fake retains the actual optional ordinary consumer name for observing roles; it does not infer child ownership from the controller. Admitted Seek completion uses the same physical child fence, resets the persistent cursor and disconnects that ordinary child before a fresh Subscribe restores delivery. GA `ServerCnx.handleAck` retains the ordinary consumer, and transaction settlement addresses its persistent subscription; leaving the scalable controller does not invalidate that already admitted work.

The public aggregate has 31 runtime parity scenarios across the baseline and advanced suites: fourteen baseline and seventeen advanced. The baseline suite also has one public type and value contract test and a Moonpool shared-connection constructor contract. The constructor contract uses `TokioProviders` and exercises lookup, lazy transaction-coordinator bootstrap, abort and client close.
They cover typed delivery, concurrent receive and atomic batch, aggregate budget, same-epoch rebalance and drain, child reconnect, ack failure/retry, nack redelivery, close wakeup, strict split/merge barriers, exact-M1 sealed-assignment drain without reopen, Strict versus BrokerManaged cross-member ancestry, vector seek/resync, transaction commit/abort/poison, terminal client shutdown during transaction registration, withdrawn opens completing during aggregate close, and controller push/baseline/incarnation ordering.
The pure aggregate model additionally proves that a completed sealed parent may disappear and reappear beside an active descendant without another parent open or suppression of the descendant open.

The Moonpool runtime additionally runs the complete aggregate over `SimProviders` controller and segment sockets for four schedules derived from `MOONPOOL_SEED`.
That provider-native test covers two-source typed delivery, aggregate status and position, acknowledgement, and close without an ambient Tokio runtime.

Focused code, fake, proto, runtime, façade, and differential suites passed for this implementation.
A local worktree invocation of `check-runtime-test-parity` reported Tokio 407 / Moonpool 407 before the final committed-diff validation pass.

The real `e2e_scalable_topic` target compiles and is discovered when the default-off `scalable-topics` product feature is enabled; it has no `#[ignore]` or separate e2e feature.
The current GA 5.0.0 broker contract covers the public builder, multi-segment typed delivery, vector acknowledgement and seek, transaction commit/abort, two-owner PIP-486 bucket sharing with individual acknowledgements, and eventual unsubscribe membership removal on both runtimes. The original M1-era test plan and its acceptance-time Docker limitation are historical; the maintained current inventory is in [docs/testing.md](../../docs/testing.md).
Because the standalone fixture uses one reachable endpoint for bootstrap and controller registration, it cannot prove same-cluster multi-broker controller routing.
The scalable transaction checks exercise coordinator id 0 through lookup and `TcClientConnect` on the bootstrap connection, matching the existing Tokio transaction path; they do not establish Java-equivalent coordinator handshakes after multi-broker relocation or reconnect.
Because the M1 broker controls when child segments enter the assignment, the real-broker Strict phase also does not isolate Magnetar's client-side ancestry gate; the fake-backed differential suites provide that evidence.

The sim-coverage report and hard-gated prefix sets now contain exactly these eight packages:

1. `magnetar-proto`
2. `magnetar-runtime-tokio`
3. `magnetar-runtime-moonpool`
4. `magnetar-differential`
5. `magnetar-auth-athenz`
6. `magnetar-auth-sasl`
7. `magnetar-driver` at `crates/magnetar`
8. `magnetar-fakes`

Execution remains exactly `-p magnetar-runtime-moonpool -p magnetar-differential`, so façade Docker e2e targets do not run under coverage.
`magnetar-admin`, `magnetarctl`, `magnetar-auth-oauth2`, `magnetar-messagecrypto`, and other uncompiled packages remain advisory `not gated` scope.
ADR-0096's invocation-owned target, artifact-flag rejection, output-only LCOV, cleanup, optimization, and exclusions are unchanged.
The record-less rule is tightened: a gated file containing a non-test function body hard-fails when it has no `SF:` record even if a sibling file proves that the crate reached LCOV; a module/export/constant/bodyless-declaration-only file remains advisory, and a wholly record-less gated crate still hard-fails.
`check-sim-coverage` computes `git diff <merge-base>..HEAD` and therefore excludes uncommitted worktree changes.
The complete implementation diff must be represented by `HEAD` and the enforcing gate rerun before a green result can be cited as acceptance evidence for this decision.

### Unresolved upstream contracts

The local hardening above does not solve or close these issues:

| Issue                                                                | Missing upstream contract                                          | Magnetar's bounded interim behavior                                                             |
| -------------------------------------------------------------------- | ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| [apache/pulsar#26272](https://github.com/apache/pulsar/issues/26272) | M1 has no logical unregister on a pooled controller connection      | M1 uses the connection-close fallback; GA stages unsubscribe after registration settles without making its response a local close gate. |
| [apache/pulsar#26273](https://github.com/apache/pulsar/issues/26273) | Assignment ordering and fencing across controller connections      | Local connection incarnations fence stale work; no distributed controller term is claimed.      |
| [apache/pulsar#26274](https://github.com/apache/pulsar/issues/26274) | Complete parent-before-child ordering across members and deep DAGs | Strict mode requires local proof; BrokerManaged explicitly weakens only the cross-member claim. |
| [apache/pulsar#26275](https://github.com/apache/pulsar/issues/26275) | Leader-only controller registration through a proxy                | Direct validated routing works; proxy-any-broker registration fails closed.                     |

`QueueConsumer` and `CheckpointConsumer` remain out of scope. M1 has no bucket ranges or unsubscribe command; the GA implementation uses PIP-486 bucket ranges and sticky children when assigned, and sends the GA unsubscribe command on close. Cross-layout vector seek and a distributed assignment generation remain out of scope.

## Consequences

Applications can now consume assigned scalable segments through one typed, engine-generic aggregate instead of observing a layout and rebuilding on every DAG change.
The strongest ordering mode is safe by construction where local history can prove ancestry, while the compatibility mode names its weaker guarantee rather than implying one.
One budget and manual FLOW prevent segment count from multiplying controlled receive memory, at the cost of lower concurrency when the configured budget admits few maximum-frame reservations.

The surface is intentionally breaking inside the default-off experimental feature.
The old topic-only async constructor and layout-observer events are replaced by a required-subscription builder, owned typed deliveries, source-qualified authority, vector positions, and explicit async close.
The hidden engine extension contract exports `RawStreamMessage`, `StreamConsumerBackend`, and `StreamConsumerOptions` with `SegmentSubscriberApi`, so downstream adapters can name its required return types.
Low-level proto users also see the new aggregate, route, position, DAG-validation, transaction, and lifecycle types.

Strict mode can block forever on cross-member ancestry, ownership drain can wait forever on an unresolved delivery, and logical close on M1 can leave broker membership behind.
Those availability costs are accepted because timing out would invent completion or release that M1 cannot prove.
BrokerManaged mode and explicit aggregate close are the caller-selected escape hatches, with their weaker ordering or at-least-once consequences documented.

## References

- [`specs/proposals/feat-m1-hardened-stream-consumer.md`](../proposals/feat-m1-hardened-stream-consumer.md) — implementation map promoted by this decision.
- [`crates/magnetar/src/scalable.rs`](../../crates/magnetar/src/scalable.rs) — public builder, typed delivery, acknowledgement, transaction, seek, status, and close surface.
- [`crates/magnetar-proto/src/stream_consumer.rs`](../../crates/magnetar-proto/src/stream_consumer.rs) — pure aggregate lifecycle, budget, ordering, authority, and action model.
- [`crates/magnetar-proto/src/stream_position.rs`](../../crates/magnetar-proto/src/stream_position.rs) — canonical `MSTR` positions.
- [`crates/magnetar-proto/src/dag_watch.rs`](../../crates/magnetar-proto/src/dag_watch.rs) — complete DAG validation and ordering eligibility.
- [`crates/magnetar-runtime-tokio/src/scalable.rs`](../../crates/magnetar-runtime-tokio/src/scalable.rs) and [`crates/magnetar-runtime-moonpool/src/scalable.rs`](../../crates/magnetar-runtime-moonpool/src/scalable.rs) — typed routes, direct controller routing, children, and provider-native operation.
- [`crates/magnetar-fakes/src/m1.rs`](../../crates/magnetar-fakes/src/m1.rs) — stateful M1 fake cluster.
- [`crates/magnetar-differential/tests/stream_consumer_equivalence.rs`](../../crates/magnetar-differential/tests/stream_consumer_equivalence.rs) and [`stream_consumer_advanced_equivalence.rs`](../../crates/magnetar-differential/tests/stream_consumer_advanced_equivalence.rs) — 31 public aggregate runtime parity scenarios, one public type and value contract test, and a Moonpool shared-connection constructor contract.
- [`crates/magnetar/tests/e2e_scalable_topic.rs`](../../crates/magnetar/tests/e2e_scalable_topic.rs) — real GA broker contract on Tokio and Moonpool.
- [`xtask/src/main.rs`](../../xtask/src/main.rs) — exact eight-package sim-coverage closure and unchanged two-root execution.
