# ADR-0099 — Non-durable reattach cursor safety

- **Status**: Accepted
- **Date**: 2026-08-11
- **Decider**: Florentin Dubois
- **Tags**: reconnect, acknowledgements, non-durable-subscription
- **Amends**: [ADR-0096](0096-reconnect-batch-and-durable-cursor-safety.md) (non-durable reattach only; every other decision stands)

## Context

ADR-0096 stopped established durable consumers from sending the highest locally submitted ack as `CommandSubscribe.start_message_id` on reattach because that value is neither broker-confirmed nor necessarily contiguous.
It deliberately retained the same value as a client-side fallback for non-durable consumers.

That exception preserves the original message-loss failure.
With a Shared or Key_Shared subscription, a consumer can submit an individual ack for a higher message while a lower message is nacked or still in flight.
If the connection fails before the ack response reaches the client, reattaching after the local maximum silently skips the lower message even though no contiguous acknowledged frontier exists.

The optional unacked and nack trackers cannot establish a confirmed frontier: they may be disabled, and `Connection::ack` removes entries before `CommandAckResponse` arrives.
Adding a confirmed contiguous cursor would therefore require new mandatory delivered-message state and richer pending-ack state.

## Decision

No consumer reattachment uses a locally submitted ack watermark.

- an established durable consumer omits `start_message_id` and defers to the broker's persisted cursor;
- a never-attached consumer retains the caller's explicit initial start position;
- a non-durable consumer reuses only that original requested start position on every reattach.

The rule applies through the shared selector to full reconnect rebuild, transient subscribe retry, and same-broker `CommandCloseConsumer` reattachment.
Seek retains its target-owned resubscribe path.

`last_acked_message_id` is removed from `ConsumerState` because reconnect was its only production consumer.
No confirmed-frontier subsystem is introduced.

## Consequences

**Safer.** A higher locally submitted ack cannot make a non-durable reattach skip a lower nacked, pending, or unconfirmed message.

**Trade-off.** A non-durable reattach can redeliver messages that the broker accepted before disconnect.
At-least-once duplication is observable and recoverable; silently skipping an unacknowledged message is not.

**Compatibility.** Public APIs and initial-subscribe behavior are unchanged.
Reader and TableView consumers, which use non-durable subscriptions, gain the conservative reconnect behavior without caller changes.
No dependency, channel, I/O in `magnetar-proto`, or clock source is introduced.

**Test contract.** The ADR-0024 layers assert the durable broker-cursor rule and the non-durable original-position rule after a higher local ack.
The real-Pulsar regression blocks broker-to-client frames, submits an ack for the higher of two unbatched messages, nacks the lower message, cuts the connection before `CommandAckResponse` reaches the client, and requires lower-message redelivery for both non-durable and durable Shared subscriptions.

## References

- Issue [#403](https://github.com/CleverCloud/magnetar/issues/403).
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — mandatory cross-runtime test layers.
- [ADR-0096](0096-reconnect-batch-and-durable-cursor-safety.md) — durable cursor safety and the superseded non-durable fallback.
