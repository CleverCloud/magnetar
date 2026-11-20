# ADR-0130 — Tell a terminal receive error from an error about one message in the push-delivery loops

- **Status**: Accepted (amends the "Clean shutdown" rule of [ADR-0064](0064-consumer-message-listener-push-delivery.md) for the single-topic poller, the wrapper poller and the `TableView` drain; exposes the receive gate of [ADR-0069](0069-bounded-transient-open-retry-and-recoverable-receive-gating.md) without changing it — everything else in ADR-0064 remains binding)
- **Date**: 2026-11-19
- **Decider**: Florentin Dubois
- **Tags**: consumer, push-delivery, listener, table-view, error-handling, runtime

## Context

[ADR-0064](0064-consumer-message-listener-push-delivery.md) specifies the push poller as `loop { let Ok(msg) = receive().await else { break }; on_message(msg); }` and justifies the `break` with "`receive()` resolving with an error (closed / terminally-disconnected consumer) breaks the loop".
That premise is false for both runtimes.
`ReceiveFut` pops a message and then post-processes it, and a failure there resolves `Err` with the consumer still open:

- an encrypted message on a consumer with no decryptor, or one that fails to decrypt, under the default `CryptoFailureAction::Fail` (`crates/magnetar-runtime-tokio/src/consumer.rs` `ReceiveFut::poll`, the `Fail` arm and the "no decryptor configured" arm; moonpool twin `PostProcessOutcome::Fail`);
- an unknown compression code, or a payload that does not decompress.

The pull contract already treats those as per-message: `crates/magnetar/tests/e2e_crypto.rs` asserts that after the `Err` the consumer is still usable.
Three loops read the same `Err` as "closed" instead, and each exits for good on one poisoned message: `spawn_listener_loop` (the single-topic poller behind `subscribe_with_listener`), `spawn_wrapper_message_listener` (multi-topic, partitioned and pattern consumers) and the `TableView` drain.
A builder push consumer then closes (its poller held the only clone), a wrapper leaves its healthy children subscribed and undrained, and a `TableView` keeps `is_connected() == true` while serving a frozen snapshot.
Nothing signals it beyond `MessageListenerHandle::is_running()`.

The wrapper poller has a second way to read an unrelated `Err` as "everything closed".
`MultiTopicsConsumer::receive` and `PatternConsumer::receive` return the error of the first failing child.
`remove_topic`, and a pattern `update` removal, drop the child from the set and then close it, while the poller is parked in `select_all` over a snapshot that still holds it, so that child's future resolves `Err(Closed)` and the whole poller exits.

The terminal states are not all visible through `Consumer::is_closed`, which reads only the slot's `closed` flag.
`ReceiveFut` gates on `consumer_handle_is_terminal(handle) || is_no_driver()` ([ADR-0069](0069-bounded-transient-open-retry-and-recoverable-receive-gating.md)).
A per-handle terminal subscribe failure (`fail_consumer_subscribe_with_broker_error`, a redirect after a bundle move) sets `terminal_failure` and never `closed`, so a loop that tested `is_closed()` would retry that receive error forever.

Alternatives considered:

1. **Break on `Err` only when `is_closed()`.** Rejected: for a consumer in one of the states above `receive()` resolves `Err(Closed)` immediately and forever while `is_closed()` stays `false`, so the loop would retry it without ever yielding — a pegged core, and a hang on a single-threaded runtime. It would trade a silent stop for a busy loop.
2. **Classify the error value (`ClientError::Closed`).** Rejected: `ConsumerApi::Error` is generic, and the wrapper consumers flatten every child error into `PulsarError::Other(format!("receive: {e}"))`, so the classification is lost exactly where the wrapper poller needs it.
3. **Stop after N consecutive errors.** Rejected: a burst of poisoned messages would still end delivery, and the bound would hide the real distinction instead of drawing it.
4. **Expose the receive gate itself and key the exit on it.** Chosen.

## Decision

Key every push loop's exit on the runtime's own terminal predicate, never on the error alone.

- **Runtime accessor.** `Consumer::is_terminal()` on `magnetar-runtime-tokio` and `magnetar-runtime-moonpool` evaluates the expression `ReceiveFut` gates on: `consumer_handle_is_terminal(handle) || shared.is_no_driver()`. It takes the global Connection mutex and, below it, the per-slot mutex (ADR-0038). A recoverable supervised reconnect window is not terminal (ADR-0069), and the state is permanent once `true`. No `magnetar-proto` change.
- **Façade trait.** `ConsumerApi::is_terminal()` is a required method of the trait, implemented by the two engines by delegation.
- **Single-topic poller and `TableView` drain.** Break on `Err` only when `is_terminal()` holds. Otherwise the error concerns the one message that `receive()` already popped: log it at `debug!` with `topic` and `error`, leave the message unacked (the same disposition the typed listener already gives a message whose schema decode fails) and read the next one.
- **Wrapper poller.** `WrapperReceiver::is_terminal()` is a new method with a default of `false`. `MultiTopicsConsumer` (hence `PartitionedConsumer`) and `PatternConsumer` implement it as "some child of the current set is terminal", over a cloned snapshot taken after the set's mutex is released. The poller breaks on `Err` only when it holds; otherwise it logs at `debug!` and goes back to the top of its loop, which re-snapshots the children and parks on `membership_changed` if the set is empty. A child the wrapper has already removed is not in the snapshot, because `remove_topic` and `update` take it out of the set before closing it, so closing a child on purpose never reads as a terminal wrapper. An empty set is not terminal.
- **Residual, kept deliberately.** A child that becomes terminal while it stays in the set still ends the wrapper push consumer, as before. `MultiTopicsConsumer::receive` and `PatternConsumer::receive` return that child's `Err` on every call and starve its siblings, so a poller that continued would retry the same error without making progress. Skipping terminal children is a change to the pull `receive()` itself, which today wedges on a terminal child; `is_terminal` for the wrappers becomes "every child" when that change lands.
- **Unchanged.** `ConsumerEventListener` keeps breaking on any `Err`: `ActiveChangeFut` resolves `Err` only on the same terminal gate and has no per-message failure to confuse it with. The pattern auto-reconcile task is driven by a ticker and exits on `is_closed()`; it never reads a `receive()` error.

## Consequences

- A poisoned message no longer ends push delivery or freezes a `TableView`; it is logged and skipped, and stays unacked so the broker can redeliver it under the subscription's own policy.
- `remove_topic` and pattern removals no longer stop push delivery for the surviving topics.
- A consumer that is genuinely terminal still ends the loop, including one whose terminal state never sets `closed`. `push_delivery_terminal_exit.rs` pins that the loops end when the connection is lost for good, so a loop that never breaks fails a test instead of burning a core. It does not tell `is_terminal()` from `is_closed()`: a plain terminal connection drop also closes the consumer slots (`fail_all_pending`), so a mutant keyed on `is_closed()` passes it. The `terminal_failure` case has no test, because it needs a broker script that exhausts a subscribe retry budget.
- API surface grows by one method on each runtime `Consumer`, one required method on `ConsumerApi` (a source break for an external implementor of that trait; both in-tree engines implement it) and one provided method on `WrapperReceiver`. An external `WrapperReceiver` whose `wrapper_receive` can fail for good must override it, or the poller retries that failure.
- Every skipped error is one `debug!` line, not a `warn!`: it is a per-message record, which [ADR-0054](0054-logging-policy.md) §7 keeps at `debug!` because a topic whose every message fails would otherwise log at send throughput. An operator who wants to see them enables the `magnetar::consumer_listener` and `magnetar::table_view` targets at `debug`. A `remove_topic` under a running poller logs one such line for the removed child.
- ADR-0024 layers: `magnetar-proto` is untouched, so layer (a) is not applicable; layers (b) and (c) are `is_terminal_tracks_the_receive_gate` in each runtime's `consumer_drop_close.rs`, which `check-runtime-test-parity` keeps 1:1; no layer (d) differential test and no `e2e_*.rs` test were added, since the change emits no new event and the e2e suite needs a Docker broker. `check-sim-coverage` (which needs `--all-features`) was not run for this change; the façade lines are measured only through `magnetar-differential`'s aggregate tests, so expect it to report the new façade lines until the differential layer exercises them. The façade behaviour is covered over a scripted loopback broker rather than a Docker broker: `push_delivery_error_survival.rs` (the four per-message and removal scenarios and their controls), `push_delivery_terminal_exit.rs`, and the `consumer_listener.rs` unit test of the wrapper poller.

## References

- `crates/magnetar/src/consumer_listener.rs` — `spawn_listener_loop`, `spawn_wrapper_message_listener`, `WrapperReceiver::is_terminal`.
- `crates/magnetar/src/table_view.rs` — `spawn_drain`.
- `crates/magnetar/src/multi_topics.rs`, `crates/magnetar/src/pattern_consumer.rs` — the `WrapperReceiver` implementations.
- `crates/magnetar/src/engine/mod.rs`, `crates/magnetar/src/engine/tokio.rs`, `crates/magnetar/src/engine/moonpool.rs` — `ConsumerApi::is_terminal`.
- `crates/magnetar-runtime-tokio/src/consumer.rs`, `crates/magnetar-runtime-moonpool/src/consumer.rs` — `Consumer::is_terminal` and the `ReceiveFut` gate it mirrors.
- `crates/magnetar/tests/push_delivery_error_survival.rs`, `crates/magnetar/tests/push_delivery_terminal_exit.rs`.
- [ADR-0064](0064-consumer-message-listener-push-delivery.md), [ADR-0069](0069-bounded-transient-open-retry-and-recoverable-receive-gating.md), [ADR-0038](0038-split-connection-mutex.md), [ADR-0054](0054-logging-policy.md).
