# ADR-0140 — Return the pattern consumer next to its listener handle from a second builder terminal

- **Status**: Proposed — this is a proposal for the maintainers to arbitrate: the terminal described below ships with this ADR so the choice can be judged on running code, and either alternative recorded under Context can still replace it. If accepted, it amends the "Pull / push mutual exclusion" rule and the pattern-child inheritance statement of [ADR-0064](0064-consumer-message-listener-push-delivery.md) for the pattern builder only; everything else in ADR-0064 remains binding.
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: pattern-consumer, push-delivery, listener, builder, facade, api

## Context

[ADR-0064](0064-consumer-message-listener-push-delivery.md) gave `PatternConsumerBuilder` a `message_listener(...)` + `subscribe_with_listener()` pair and decided that pattern children discovered after subscribe inherit the listener.
Inheritance is implemented and holds: the wrapper poller races its in-flight `receive()` against the consumer's membership-change `Notify`, so a child that joins the set is drained on the next iteration.

What makes a child join is a separate matter, and it is caller-driven.
After the first snapshot, `PatternConsumer::update` is the only code that takes the PIP-145 deltas addressed to the consumer's watcher and subscribes the new topics.
`PatternConsumer::start_auto_reconcile` is an opt-in ticker that calls `update`; it takes the client as an `Arc<PulsarClient<E>>`, which the builder does not hold.
The poller never reconciles (`crates/magnetar/src/consumer_listener.rs`, `spawn_wrapper_message_listener`).

`subscribe_with_listener()` builds the `PatternConsumer` inside the call, moves it into the poller and returns only the `MessageListenerHandle`.
Its caller holds nothing to call `update` or `start_auto_reconcile` on, so a pattern push consumer built that way stays on the topics matched at subscribe for its whole life.
The terminal's documentation said otherwise.
It promised that later topics inherit the listener and advised the caller to "hold an extra `PatternConsumer` clone before `subscribe_with_listener`", which cannot be followed: the builder consumes itself and no consumer exists before the call.
`README.md` and `ARCHITECTURE.md` repeated the inheritance claim without its condition.

A route that works was already public, and it is the one the end-to-end inheritance test uses: `subscribe()`, then `spawn_wrapper_message_listener` on a clone (`crates/magnetar/tests/e2e_wrapper_message_listener.rs`).
The defect is the builder terminal and its documentation.

Alternatives considered:

1. **Return the consumer next to the handle.**
   Chosen, as a new terminal beside the existing one.
   Changing the return type of `subscribe_with_listener()` instead would break every caller of it and the symmetry of the five listener terminals.
2. **Accept an auto-reconcile interval on the builder and start the ticker inside the terminal.**
   Not chosen here.
   It is the closest to Java, where `PatternMultiTopicsConsumerImpl` reconciles on its own, and it keeps the consumer out of the caller's hands.
   It also decides more than the defect asks: who owns and stops the ticker (it would live in, or beside, the `MessageListenerHandle`), what the builder borrows (`start_auto_reconcile` needs an owned `Arc<PulsarClient<E>>` and the builder holds `&PulsarClient<E>`), and how the ticker is spawned and paced (`start_auto_reconcile` uses `tokio::spawn` and `tokio::time::interval`).
   A caller that wants `update` at a moment of its own choosing is not served by it.
3. **Drop the inheritance claim from the documentation and change no code.**
   Not chosen alone.
   The corrected documentation is part of this change whichever way the arbitration goes, but a builder push consumer would still have no way to follow later topics short of leaving the builder route.

## Decision

`PatternConsumerBuilder` gains a second listener terminal, and the first one is documented for what it does.

- `subscribe_with_listener_and_consumer()` returns `(PatternConsumer<_>, MessageListenerHandle)`.
  It refuses a builder with no listener with `PulsarError::Config` before anything is sent, subscribes exactly as `subscribe()` does, hands a clone of the consumer to `spawn_wrapper_message_listener` and returns the consumer itself.
  The caller's consumer and the poller's are two handles on one child set.
- Discovery stays caller-driven.
  Nothing reconciles on its own: the caller calls `update`, or `start_auto_reconcile`, on the returned consumer, and a topic one of them subscribes reaches the listener through the existing membership-change race.
- `subscribe_with_listener()` keeps its signature, its body and its behaviour.
  Its documentation now says that it delivers the topics matched at subscribe and only those, and points to the new terminal.
- The other four listener terminals are unchanged.

## Consequences

- A caller of the builder route can follow topics created after subscribe without leaving the builder: one more call, on a value the terminal now returns.
- **Pull and push are not exclusive by construction for this terminal.**
  ADR-0064 enforces Java's "no `receive()` with a listener" rule by moving the consumer into the poller.
  A caller of the new terminal holds a consumer whose `receive()` competes with the poller, each message going to one of the two.
  The rule becomes a documented contract there, as it already was for a caller of `spawn_wrapper_message_listener` who kept a clone.
- **Teardown has an order.**
  `PatternConsumer::close` returns `Ok(())` without closing any child while another clone is alive, and the poller holds one: the caller closes the handle first, then the consumer.
  Dropping or closing the handle alone stops delivery and leaves every subscription open, where on the handle-only terminal it drops the last clone and stages the best-effort close.
- The returned consumer is also the first handle a builder push caller has for `PatternConsumer::ack`.
  The listener is set before the consumer exists, so a callback reaches the consumer through a slot the caller fills after the terminal returns, or the caller acknowledges outside the callback.
- API surface: one added method, no changed signature, no new type.
  It is additive for every existing caller.
- `MultiTopicsConsumerBuilder::subscribe_with_listener` has the same shape: its documentation says topics added by `add_topic` are picked up, on a terminal that returns no consumer to call `add_topic` on.
  It is not changed here, and the same question is open for that builder once this one is arbitrated.
- If the maintainers prefer alternative 2, the new terminal can stay as the manual route beside it or be removed before a release contains it.
  If they prefer alternative 3, it is removed and the corrected documentation stays.
- ADR-0024 layers, stated as they are rather than as the policy would have them.
  (a) None: `magnetar-proto` is untouched, and the terminal composes `subscribe()` and `spawn_wrapper_message_listener`, which exist.
  (b) and (c) None: the runtime crates do not depend on the façade and cannot reach a builder.
  (d) `crates/magnetar-differential/tests/pattern_listener_consumer_equivalence.rs` runs one scenario on `PulsarClient<TokioEngine>` and on `PulsarClient<MoonpoolEngine<TokioProviders>>`, each against its own scripted loopback broker, and requires the same trace from both: the refusal of a builder with no listener before anything is sent, the handle-only terminal on a topic matched at subscribe, and a topic announced after subscribe delivered to the listener once the caller's `update` on the returned consumer has subscribed it.
  The Moonpool leg runs on host sockets and not in a seeded simulation, because `spawn_wrapper_message_listener` starts the poller with `tokio::spawn` on both engines and `SimProviders` has no Tokio runtime to host it.
  No `e2e_*.rs` test was added; a real broker reaches the path, and `e2e_wrapper_message_listener.rs` already covers the same delivery through `subscribe()` and `spawn_wrapper_message_listener`.
- What that suite proves is bounded.
  A test of an added method cannot fail before the method exists, so it pins the one property a wrong implementation would miss: the returned consumer is the one the listener is registered on.
  A terminal that returned any other consumer on the same pattern would report the same reconcile and the same topic list, and deliver nothing.
  It does not assert discovery without a caller action, which would be alternative 2.

## References

- `crates/magnetar/src/pattern_consumer.rs` — `PatternConsumerBuilder::subscribe_with_listener`, `subscribe_with_listener_and_consumer`, `PatternConsumer::update`, `start_auto_reconcile`, `close`.
- `crates/magnetar/src/consumer_listener.rs` — `spawn_wrapper_message_listener`, `MessageListenerHandle`, and the module sections on pull / push exclusion and pattern-child inheritance.
- `crates/magnetar-differential/tests/pattern_listener_consumer_equivalence.rs`, `crates/magnetar/tests/e2e_wrapper_message_listener.rs`.
- `README.md` (parity matrix, `messageListener` and pattern consumer rows), `ARCHITECTURE.md` (push delivery).
- [ADR-0064](0064-consumer-message-listener-push-delivery.md), [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md), [ADR-0132](0132-address-pip-145-topic-list-deltas-to-their-watcher.md).
