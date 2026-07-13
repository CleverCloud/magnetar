# ADR-0076 — Conserve flow permits across chunk reassembly

- **Status**: Accepted
- **Date**: 2026-07-13
- **Decider**: Florentin Dubois
- **Tags**: consumer, chunking, pip-37, flow-control, java-parity, issue-331

## Context

[Issue #331](https://github.com/CleverCloud/magnetar/issues/331) reports direct single-partition Failover consumers that stop making progress with backlog remaining and broker-side `availablePermits = 0`.
The reported consumers use a receiver queue of 2,000 and share one subscription across twelve direct `{topic}-partition-N` child topics.

Pulsar accounts flow permits in broker dispatch units, not in the logical messages exposed after client-side processing.
Each chunk is one broker entry with an effective batch size of one, so [`AbstractBaseDispatcher::filterEntriesForConsumer`](https://github.com/apache/pulsar/blob/v4.2.3/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/AbstractBaseDispatcher.java#L241-L290) adds one message to the dispatch total and [`Consumer::sendMessages`](https://github.com/apache/pulsar/blob/v4.2.3/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/Consumer.java#L428-L430) subtracts that total from the consumer permit budget.
The Java client compensates in [`ConsumerImpl::processMessageChunk`](https://github.com/apache/pulsar/blob/v4.2.3/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java#L1581-L1585) by returning permits during reassembly, leaving one permit to be returned when application code consumes the completed logical message.

Magnetar previously returned only that final logical-message permit.
`ConsumerState::deliver` accepted an incomplete chunk into `chunk_reassembly` and returned `DeliverOutcome::Buffered` without increasing `consumed_since_flow`, while `ConsumerState::pop_message` increased the counter once after reassembly.
An `N`-chunk logical message therefore spent `N` broker permits but returned one.
If the broker exhausted its grant before enough logical messages reached the half-queue refill threshold, the next chunk could not arrive, the current logical message could not complete, and no later pop could trigger `CommandFlow`.

Mirroring Java's numeric non-final-`chunk_id` check exactly would conserve the total for a completed message, but it would couple refund timing to metadata position rather than current reassembly state.
Magnetar already accepts valid out-of-order chunks after the first chunk has opened a buffer, so the state-machine outcome—accepted but still incomplete versus accepted and complete—is the direct conservation boundary.

## Decision

Conserve one permit for every accepted chunk dispatch unit inside the shared sans-I/O consumer state.

- `ConsumerState::record_broker_permit_consumed` is the single internal saturating increment of `consumed_since_flow`; replicated-subscription marker accounting uses the same operation.
- When an accepted chunk leaves reassembly incomplete, `ConsumerState::deliver` records its permit immediately before returning `DeliverOutcome::Buffered`.
- The chunk that completes reassembly does not record an immediate permit because it creates one queued logical message; `ConsumerState::pop_message` records that remaining permit when user code consumes the message.
- `Connection` calls `ConsumerState::maybe_flow` after inbound delivery as well as after logical-message pop, so an intermediate-chunk refund can emit before a complete message is available.
- The connection stages the permit count while holding the per-consumer slot lock, releases that guard, and only then encodes `CommandFlow`, preserving the global-connection-before-per-slot ordering from [ADR-0038](0038-split-connection-mutex.md).
- Malformed, duplicate, expired, and evicted chunk acknowledgement/redelivery behavior is unchanged; an already accepted incomplete chunk retains its refund if its buffer is later removed.
- The existing subscribe-time flow grants remain unchanged because reducing the initial receive window is a separate behavior decision.

Across a valid `N`-chunk message, `N - 1` permits are therefore recorded during incomplete reassembly and one is recorded at logical consumption.
The decision lives in `magnetar-proto`, so Tokio and Moonpool inherit identical behavior without engine-specific flow logic.

## Consequences

- Chunked consumers cannot permanently drain broker permits merely because several broker entries collapse into one user-visible message.
- Valid out-of-order arrival remains supported because accounting follows whether the accepted chunk completed reassembly, not its numeric position.
- `CommandFlow` may now be staged during inbound delivery before any logical message is available, which is required to let the broker send the remaining chunks.
- There is no public API, wire-format, channel, timer, or I/O-dependency change.
- This complements [ADR-0063](0063-bounded-chunk-reassembly.md), which bounds chunk-buffer memory and lifetime, and [ADR-0071](0071-pluggable-receiver-queue-policy.md), which defines receiver-queue targets and refill thresholds; neither decision is superseded.
- The behavior ships with the five [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) layers: a sans-I/O protocol regression, matched Tokio and Moonpool integration regressions, differential equivalence, and the Docker end-to-end issue #331 topology.

## Verification contract

The end-to-end regression uses twelve direct Failover child-topic consumers on one shared subscription with receiver queues of 2,000.
It publishes 900 five-chunk messages to one partition and 1,100 small messages to each of the other eleven partitions, then requires every consumer to drain and close cleanly.
The unchanged implementation was reproduced against Pulsar 4.0.4 with the chunked partition stopping at 800 of 900 logical messages, `availablePermits = 0`, and residual backlog while the other eleven partitions drained.
The maintained suite defaults to Pulsar 4.2.3 and retains `MAGNETAR_PULSAR_IMAGE_TAG` for explicit compatibility runs.

## References

- `crates/magnetar-proto/src/consumer.rs` — accepted-incomplete accounting and the shared permit counter.
- `crates/magnetar-proto/src/conn.rs` — delivery-time `maybe_flow` staging and post-lock `CommandFlow` encoding.
- `crates/magnetar-runtime-{tokio,moonpool}/tests/consumer_flow_control_edge.rs` — one-for-one runtime regressions.
- `crates/magnetar-differential/tests/failover_active_reflow_equivalence.rs` — cross-engine grant and reassembly equivalence.
- `crates/magnetar/tests/e2e_batch_chunk.rs` — faithful issue #331 broker regression and Pulsar image override.
- [`ARCHITECTURE.md`](../../ARCHITECTURE.md#consumer-flow-permit-accounting) — current state-machine and lock-staging contract.
- [`docs/testing.md`](../../docs/testing.md#end-to-end-docker) — maintained end-to-end topology and broker-version policy.
