# ADR-0018 — PIP-188 `TOPIC_MIGRATED` → supervised reset + reconnect

- **Status**: Accepted
- **Date**: 2026-05-21
- **Decider**: Florentin Dubois
- **Tags**: pip-188, reconnect, supervisor, ha, java-parity

## Context

[PIP-188](https://github.com/apache/pulsar/wiki/PIP-188:-Topic-migration) adds a broker-driven `CommandTopicMigrated` frame.
When a topic moves between clusters (e.g. for ops-driven rebalancing), the broker emits `TOPIC_MIGRATED` with the new `brokerServiceUrl` / `brokerServiceUrlTls`.
The client is expected to:

1. Close its in-flight ops on the old broker.
2. Reconnect to the URL announced in the event.
3. Re-subscribe / re-produce as if it were a fresh client.

Magnetar already decoded the wire opcode (commit `7d568f9`) and surfaced the `TopicMigrated` event from `Connection::poll_event`, but the driver dispatched it as a logged-only no-op.
The parity matrix listed PIP-188 as `🟡`.

Constraints from prior ADRs:

- [ADR-0003 no-channels-rule](0003-no-channels-rule.md): no `oneshot` to signal "please rebuild now".
- The supervisor is already in place (commit `afda625`) and rebuilds producers + consumers (commit `cc465d9`).
  It triggers on `Connection::reset()` returning a `ClientError`.

## Decision

In `crates/magnetar-runtime-tokio/src/driver.rs`, the `TopicMigrated { new_service_url }` event arm:

1. Logs the migration at `INFO` with the old + new URLs.
2. If the engine has a `ServiceUrlProvider` plumbed via [ADR-0016 PIP-121](0016-pip-121-cluster-failover.md), records the new URL into it (via downcast to `ControlledClusterFailover` or by replacing a static provider).
   This is opportunistic — the provider trait does not require a setter.
3. **Returns `ClientError::TopicMigrated(new_service_url)`** from the driver loop.
   The supervisor catches the error, calls `Connection::reset()`, applies the backoff schedule, and re-handshakes on the new URL (read from the provider on the next reconnect attempt).
4. The standard rebuild path (commit `cc465d9`) re-issues `CommandProducer` / `CommandSubscribe` on the new connection so in-flight producers and consumers transparently resume.

No new state machine in `magnetar-proto`.
The wire event is already there; the only behavioural change is the driver-loop arm returning `ClientError` instead of swallowing the event.

## Consequences

- Topic migrations look identical to a transient disconnect to user code — the producer / consumer surface doesn't observe the reconnection.
- The `Backoff` struct (already in `magnetar-proto`) governs the reconnect cadence — same path as a network-level drop.
- A migration to a URL that fails to handshake degenerates to the same retry-and-give-up loop as any other reconnect.
  The supervisor's max attempts cap applies.
- This pairs cleanly with PIP-121: if the user has supplied an `AutoClusterFailover`, a `TOPIC_MIGRATED` to a known-failed URL would ride the failover policy on the next probe round.

## References

- `crates/magnetar-runtime-tokio/src/driver.rs` — `TopicMigrated` arm in the event loop
- `crates/magnetar-proto/src/conn.rs` — `Connection::reset()` (Stage 2 supervisor primitive)
- Commit `7d568f9` — "feat(proto): PIP-188 TopicMigrated event surfaced from CommandTopicMigrated"
- Commit `9a35db4` — "feat(pip-188): handle TopicMigrated by triggering supervised reset+reconnect"
- Commit `afda625` — "feat(supervisor): wire reconnect into tokio driver_loop (Stage 2)"
- Commit `cc465d9` — "feat(supervisor): rebuild producers + consumers across reconnect (Stage 3)"
- Apache PIP-188
- [ADR-0016 pip-121-cluster-failover](0016-pip-121-cluster-failover.md)
