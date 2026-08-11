# ADR-0106 — Re-attach a broker-closed producer in place on a live connection

- **Status**: Accepted
- **Date**: 2026-09-17
- **Decider**: Florentin Dubois
- **Tags**: producer, recovery, sans-io, bundle-unload, issue-451

## Context

[Issue #451](https://github.com/CleverCloud/magnetar/issues/451): magnetar 1.7.1 in production, 2026-09-14.
One partition of a partitioned topic was unloaded.
From that moment every publish the round-robin router sent to that partition failed `send rejected: code=-1 message=send timeout` after the configured 60 s, forever, with no log line recorded at close time.
Nothing recovered until the process restarted.

A broker `CommandCloseProducer` does not imply a transport drop.
`ServerCnx.closeProducer` runs `safelyRemoveProducer` and then writes the frame on a connection that keeps serving every other producer and consumer on it; `handleSend` for the detached id then lands in the `recentlyClosedProducers` path and is silently ignored, while any OTHER unknown producer id closes the whole connection with "producer is not ready".
`pulsar-admin topics unload`, a bundle split, and load shedding all produce exactly this shape.

The `Type::CloseProducer` arm of `Connection::handle` did only half the recovery.
It set `broker_ready = false` — correct, and load-bearing, because a `CommandSend` reaching a not-ready producer costs the whole connection — and pushed `ConnectionEvent::ProducerClosedByBroker`.
It never re-emitted `CommandProducer`.
Task #56 had established the other half: keep `closed = false` so `rebuild_producers` can replay the producer, because marking it closed makes that sweep filter it out and the next user `send()` surface `ProducerError::Closed`.
But `rebuild_producers` runs only after a new-session handshake (`pending_rebuild` on the tokio driver, its moonpool equivalent), and the ADR-0080 retry leg is armed only by `DriverRetry::Producer`, which the `CommandError` handler pushes solely for a still-pending `ProducerOpen` — `ProducerSuccess` clears `open_request_id`, and a close creates no pending open at all.
With the connection up, neither ever fires.

The slot therefore sat at `closed = false, broker_ready = false, open_request_id = None` for the life of the connection.
`drain_producer_outbound` and `drain_producer_outbound_vectored` skip a slot while `!broker_ready`, so every staged publish waited until the `handle_timeout` sweep resolved it with the `SEND_TIMEOUT_CODE = -1` / "send timeout" sentinel.
`PartitionedProducer::pick_partition` has no readiness input and `Producer::is_connected` is connection-level, so `1/N` of all publishes kept landing on the dead child.
The queued `ProducerClosedByBroker` had no reader: both driver `poll_event_if` allow-lists omit it, and the only consumers — `EventWaitFut` on tokio, `ProducerReadyFut` on moonpool — are parked exclusively inside the open operation, which has already returned the assembled `Producer`.

The consumer side had the same root cause and was fixed in [issue #307](https://github.com/CleverCloud/magnetar/issues/307): its `Type::CloseConsumer` arm calls `emit_in_place_consumer_resubscribe` on the same socket and suppresses the event.
The producer side had no analogue.

Java's behaviour is the in-place re-attach, not a reconnect.
`ClientCnx.handleCloseProducer` removes the producer id, derives an optional `hostUri` from `assigned_broker_service_url[_tls]`, and calls `producer.connectionClosed(this, delay, hostUri)` unconditionally — the channel staying up changes nothing.
`ConnectionHandler.connectionClosed` then releases the connection, CASes it to null, and schedules `grabCnx(hostUrl)` after `0 ms` when a URL was assigned and `backoff.next()` otherwise; `connectionOpened` re-sends `CommandProducer` with a bumped epoch and `handleProducerSuccess` calls `resendMessages`.
`PersistentTopic.close` attaches `ExtensibleLoadManagerImpl.getAssignedBrokerLookupData` to every non-transferring close, which is non-empty exactly when the Extensible Load Manager is enabled and `loadBalancerMultiPhaseBundleUnload` is on — so on an ELM cluster a plain `topics unload` yields `assigned_broker_service_url = Some(url)`, and a ModularLoadManager or standalone unload yields `None`.
Which load manager the production cluster runs is UNVERIFIED.
`BrokerServiceException` maps `ServiceUnitNotReadyException` and `TopicFencedException` to `ServerError.ServiceNotReady`, so a `CommandProducer` arriving while the bundle reloads is answered `ServiceNotReady` — which makes the rejected first re-attach the primary real-world path, not a corner case.

### Alternatives considered

- **Drain `ProducerClosedByBroker` in both drivers' allow-lists and spawn a retry leg there.** Two copies of the same bookkeeping to keep in step, a race with the open waiters over the same event, and it can re-home nothing.
- **Reuse `DriverRetry::Producer` from the close arm.** It requires a `failed_request_id` equal to the slot's `open_request_id`, which is `None` after a successful open. There is nothing to bind the leg to.
- **Force a supervised reconnect, as `TopicMigrated` does.** Tears down every healthy producer and consumer on the socket to recover one unloaded partition, and is not Java's behaviour.
- **Re-home the producer to another pooled connection, Java-faithfully.** No proto or pool primitive exists for it: a `Producer` is pinned to one `Arc<ConnectionShared>` for its lifetime, and the retry leg's `lookup_then` terminalizes a `Redirected` outcome. Deferred to a follow-up ADR.
- **Have the partitioned router skip a detached child** (the issue's own expectation 5). Not Java parity — `PartitionedProducerImpl.internalSendWithTxnAsync` routes with no connectivity check and `isConnected()` is an `allMatch` — and it needs a per-slot readiness accessor on `ProducerApi`, which today exposes only `is_connected`. Its own ADR.
- **Back off before the FIRST re-issue, Java-style.** The `ServiceNotReady` retry leg already backs off. A proto-side timer here would be a new mechanism for no gain.

## Decision

A broker `CommandCloseProducer` that arrives on a live connection re-attaches that producer **in place, on the same socket**, in the sans-io layer so both engines inherit it with no per-engine bookkeeping to drift.

- A new private `Connection::emit_in_place_producer_reattach` re-emits `CommandProducer` for the same producer id through the existing `retry_producer_open_inner`, which bumps `epoch` so the broker recognises the open as that registration's successor, allocates the request id, registers `PendingRequestKind::ProducerOpen`, and sets `open_request_id`.
- Eligibility is `!closed && has_ever_attached && open_request_id.is_none()`, read from the already-locked slot by `producer_reattach_in_place_is_eligible` and never reaching back for the connection mutex (ADR-0038 lock ordering). `closed` means the user owns the handle; `!has_ever_attached` means the routing-aware client open loop still owns the first attachment; `open_request_id.is_some()` means an open is in flight and a second `CommandProducer` for the same id would leave one of the two replies unmatched.
- The send-drain gate stays shut (`broker_ready = false`) until the broker's fresh `CommandProducerSuccess`, whose arm then runs `replay_pending_outbound`, replays the reset snapshots, flips the gate, resets `transient_open_attempts`, and records the ADR-0028 anti-thrash `ReAttachOk`. Staged publishes keep their ORIGINAL `enqueued_at` deadline, so a configured `send_timeout` keeps ticking across the re-attach exactly as Java's does.
- `assigned_broker_service_url = Some(url)` takes the **same** in-place path, unlike the consumer twin. No existing path owns `ProducerClosedByBroker { assigned_broker_service_url: Some(_) }` on a live socket — the driver's migration arm keys on `ConnectionEvent::TopicMigrated` — and an ELM multi-phase unload makes `Some(url)` the default close shape. Java uses the URL only as a dial hint and re-sends `CommandProducer` regardless. When it names this connection's broker the in-place open succeeds; otherwise it fails bounded through the retry leg. The URL is logged, truncated through the proto-local `log_fields::truncate_broker_str`.
- A retryable rejection of the re-attach — `ServiceNotReady` while the bundle reloads, the expected answer — rides the existing ADR-0080 leg unchanged: the `CommandError` arm closes the gate, bumps `transient_open_attempts`, pushes `DriverRetry::Producer`, and the driver sleeps `delay_after_failure` (2 s by default), looks the topic up on the same connection, and re-issues at the next epoch. Expected recovery on a real unload is roughly 2 s plus two round trips, consuming one retry attempt per unload, reset on success. Past `OperationRetryConfig::max_retries` (8 by default) `fail_producer_open_with_broker_error` terminalizes the slot so parked sends resolve `Err` instead of hanging. A non-retryable answer — `ProducerFenced`, `TopicNotFound` — terminalizes immediately.
- One `warn!` per close records `handle`, `topic`, `request_id`, `epoch`, and the truncated assigned URL (ADR-0054 row 42). Java logs this at INFO; the level here is the ADR-0054 row assignment, not parity.
- `ProducerClosedByBroker` is surfaced **only while an open is in flight**, where a parked `EventWaitFut` / `ProducerReadyFut` owns that open's outcome and can read it. For a user-closed or unknown handle nothing is pushed: no reader can ever consume it, and one event per partition under bundle churn would only grow the queue. The gate is still shut for any known handle, eligible or not.

## Consequences

- The production symptom is gone: a `topics unload` of one partition costs a bounded re-attach instead of a permanent per-partition publish outage, and the close is now visible in the log.
- **A cross-broker bundle move is degraded, not fixed.** The retry leg re-issues on the same connection and terminalizes on a lookup redirect or after the retry budget. Publishes then fail `Err` rather than hanging, and the application must re-create the producer. A pool-level re-home needs its own ADR.
- **A pending `ProducerOpen` still carries no deadline** — `handle_timeout` sweeps only `PendingRequestKind::Ack`. A re-attach the broker never answers leaves the slot at `broker_ready = false, open_request_id = Some(_)`, reproducing the old symptom and making later closes ineligible. Shared with the #307 consumer path and with every reconnect rebuild; recorded, not fixed here.
- **At-least-once across the re-attach.** `drain_timed_out_sends` pops only `pending`, so a timed-out publish's staged frame is still flushed once the gate reopens, and a publish already receipted-pending at close time is replayed by `replay_pending_outbound`. Same shape as the ADR-0080 reconnect path. Batched frames already on the wire before the close carry no `replay_frames` (ADR-0096) and resolve only through `send_timeout`.
- **A broker that keeps closing is not rate-limited.** Each close triggers one `CommandProducer`, and a success resets `transient_open_attempts`, so the ADR-0080 budget never trips on a close streak. Identical exposure to the #307 consumer path; a streak budget in the shape of ADR-0103 is the answer if a storm is ever observed.
- `emit_command_producer` still sends `topic_epoch: None`, so for `Exclusive`, `WaitForExclusive`, and `ExclusiveWithFencing` producers the broker arbitrates the re-attach and may answer `ProducerFenced`, which terminalizes the slot. Parity with Java on that path is UNVERIFIED.
- **Behaviour change for a direct `magnetar-proto` driver.** `ProducerClosedByBroker` no longer surfaces for an established producer (any URL) or for a closed/unknown handle. No in-tree consumer depends on it; downstream code driving `Connection` itself could.
- The residual `ProducerOpenFailed` / `ProducerOpenFailedTransient` events pushed for an established producer still have no reader. Pre-existing ADR-0080 shape, untouched.
- The consumer arm's own `Some(url)` behaviour is deliberately left as it is: that producer-side argument does not transfer without its own evidence.

## References

- `crates/magnetar-proto/src/conn.rs` — the `Type::CloseProducer` arm, `emit_in_place_producer_reattach`, `producer_reattach_in_place_is_eligible`, and the proto unit tests.
- `crates/magnetar-runtime-tokio/tests/producer_broker_close_reattach.rs`, `crates/magnetar-runtime-moonpool/tests/producer_broker_close_reattach.rs` — the two engine legs, 1:1 (ADR-0024).
- `crates/magnetar-differential/tests/broker_close_producer_reattach_equivalence.rs` — tokio ↔ moonpool reaction parity for the suppressed event and the epoch on the wire.
- `crates/magnetar/tests/e2e_producer_partition_unload_reattach.rs` — `pulsar-admin topics unload` against a real broker.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the layered test policy this change ships against.
- [ADR-0054](0054-logging-policy.md) — the log row and the broker-string truncation.
- [ADR-0080](0080-configurable-operation-retry-policy.md) — the retry leg the rejected re-attach rides.
- [ADR-0100](0100-close-cancelled-producer-open-before-retry.md) — the successor-epoch rule the re-attach follows.
- [ADR-0103](0103-bounded-automatic-consumer-stall-recovery.md) — the sans-io-layer rationale, and the streak-budget shape a close storm would need.
