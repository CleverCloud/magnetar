# ADR-0069 — Bounded transient producer-open / subscribe retry + recoverable-vs-terminal receive gating

- **Status**: Accepted
- **Date**: 2026-06-22
- **Decider**: Florentin Dubois
- **Tags**: reconnect, resilience, runtime, sans-io

## Context

Two defects share one root cause: a transient, recoverable state was treated as terminal, stranding a user handle with no error surfaced.

### Defect #302 — one-shot transient recovery gave up forever

When the broker bounced a `CommandProducer` / `CommandSubscribe` with a transient code (`ServiceNotReady`, `MetadataError`, `TopicNotFound` — the post-`docker restart` "bundle not served, please redo the lookup" window), `magnetar-proto` emitted a recoverable `ProducerOpenFailedTransient` / `SubscribeFailedTransient` event and RETAINED the handle state ([ADR-0028](0028-supervised-reconnect-anti-thrash-policy.md) lineage).
Each engine answered with a **single** detached retry leg: `sleep(2s)` → re-lookup → one `retry_producer_open` / `retry_consumer_subscribe`.
That leg gave up permanently on any "unexpected" lookup outcome and never re-armed on a repeated transient rejection.

The proto trap made the give-up silent: a transient producer-open closes the per-slot `broker_ready` drain gate (`drain_producer_outbound` refuses to flush staged frames while `!broker_ready`), so a `send()` queued behind it stayed `Poll::Pending` forever — no error, no progress.
For consumers, a given-up retry left `available_permits = 0`, so `receive()` blocked forever.

### Defect #299 — `receive()` treated the recoverable `Failed` window as terminal

`ReceiveFut::poll` resolved `Err(ClientError::Closed)` whenever `Connection::is_closed()` was true.
`is_closed()` returns `true` for `HandshakeState::Failed`, and a transport drop sets `Failed` for the ENTIRE supervised backoff + redial + re-handshake window.
`Connection::reset()` drains + wakes the parked receive wakers WHILE the connection is still `Failed`, so a `receive()` outstanding across the drop was woken, re-polled during `Failed`, hit the `is_closed()` guard, and resolved `Err(Closed)` — even though the supervisor was actively reconnecting and the rebuild would have replayed `CommandSubscribe`.

The two fixes interlock: #302 introduces a genuine terminal-failure surface, and #299's receive guard must let that terminal surface through while re-parking during a recoverable reconnect window.

## Decision

### 1. Bounded transient-open retry with a per-handle attempt counter (sans-io)

`magnetar-proto` tracks a per-handle attempt counter (`ProducerState::transient_open_attempts` / `ConsumerState::transient_subscribe_attempts`), bumped on each transient rejection and reset to `0` on the re-attach ack (`CommandProducerSuccess` / subscribe `Success`).
Once it crosses `MAX_TRANSIENT_OPEN_RETRIES` (8) the state machine STOPS emitting the recoverable `*Transient` event and instead installs a TERMINAL failure (see §2).
The engines size their exponential-backoff sleep off the same counter (`transient_retry_delay`, seeded from the original 2 s as the first step, doubling, capped at 8 s), on the **injected clock** (ADR-0011): tokio sleeps on `tokio::time`, moonpool on the injected `TimeProvider`.
The attempt cap together with the 8 s backoff cap bounds the worst-case give-up window to `~2+4+8×6 ≈ 54 s`, the same ballpark as Java's default 30 s `operationTimeout` give-up.

The re-arm is event-driven: each repeated transient rejection re-emits the `*Transient` event, which re-spawns the one-shot leg — so the loop lives across driver iterations with no engine-side retry-in-flight state and no risk of two concurrent legs (the broker serializes rejections behind each leg's `retry_*` round-trip).

### 2. Terminal give-up methods that surface `Err` (sans-io)

`Connection::fail_producer_open(handle, reason)` drains + terminalizes every staged / in-flight `OpSend` for the handle (`OpOutcome::Terminal`), flips the slot `closed` flag, wakes each parked send waker, drops the producer state, and pushes `ProducerOpenFailed` — the per-handle scoped counterpart of `fail_all_pending` step (2) ([ADR-0055](0055-bit-flip-survivability-model.md) §1).
`send()` then resolves `Err(PeerClosed)` instead of hanging behind the closed drain gate.

`Connection::fail_consumer_subscribe(handle, reason)` sets a per-consumer `terminal_failure: Option<String>` marker, zeroes `available_permits`, drains + wakes every parked receive waker, drops the per-session subscribe request (so a reconnect rebuild does not re-attach a terminally-failed consumer), and pushes `SubscribeFailed`.
The `ConsumerState` slot is RETAINED so a parked `receive()` future can read the marker on re-poll and resolve `Err`.

Both preserve the ADR-0038 lock order (global mutex held by `&mut self`; per-slot mutex taken below it; wakers fire after the slot guard drops) and stay zero-I/O (ADR-0004).

### 3. Recoverable-vs-terminal receive gating (sans-io predicate + engine latch)

`Connection::is_terminally_closed()` is `true` only for a genuinely terminal state: `is_user_closed()` OR (`Failed` AND no supervisor configured).
It is `false` for a supervised `Failed` window and for the post-`reset()` `Uninitialized` window — both recoverable.

`Connection::consumer_handle_is_terminal(handle)` folds in the per-handle terminal failure: `is_terminally_closed()` OR the consumer slot is closed/removed OR a `terminal_failure` marker is installed (§2).

The two engines' `ReceiveFut` terminal guards switch from `is_closed()` to `consumer_handle_is_terminal(handle) || shared.is_no_driver()`:

- `consumer_handle_is_terminal` re-parks during a recoverable `Failed`/`Uninitialized` window (issue #299) and resolves `Err` on a per-handle terminal failure (issue #302);
- the engine `no_driver` latch (ADR-0059) covers the supervised give-up case, where the connection is still `Failed` with a supervisor configured but `fail_all_pending` + `mark_no_driver` have fired — `consumer_handle_is_terminal` alone cannot tell that apart from a mid-reconnect `Failed`.

The producer `send()` side already distinguishes recoverable vs terminal correctly: a `Send` key stays `Pending` while staged behind `broker_ready = false` (recoverable), and resolves `Err(PeerClosed)` on the `OpOutcome::Terminal` that `fail_producer_open` installs — so no producer-side guard change is needed.

## Consequences

- A bundle reshuffle that resolves within ~54 s is recovered transparently; a permanently-fenced bundle terminalizes the open / subscribe and surfaces `Err` to the caller, so caller-side rebuild logic can fire instead of the handle hanging forever.
- A `receive()` outstanding across a supervised drop re-parks during the recoverable `Failed` window and resolves with the post-reconnect message, never `Err(Closed)` (issue #299).
- The retry stays a spawned task coordinating via `Arc<Mutex>` + `Notify` + `Waker` (no channel — ADR-0003); `magnetar-proto` stays sans-io zero-I/O (ADR-0004); the backoff sleeps use injected clocks only (ADR-0011); the lock order is global → per-slot (ADR-0038).
- ADR-0024 layers ship in the same commit: a `magnetar-proto` unit test (bounded retry re-arm across multiple failures + terminal give-up waking the parked send/receive waker; recoverable-vs-terminal predicate for both branches), tokio + moonpool integration tests (producer give-up, subscribe give-up, receive-across-supervised-drop — kept 1:1, 279/279), a `magnetar-differential` equivalence test (both engines give up identically at the cap), and e2e (`e2e_driver_mid_session_reject.rs` producer-open give-up behind a frame-forging gate; `e2e_reconnect.rs` receive-outstanding-across-restart).

## References

- [ADR-0003](0003-no-channels-rule.md) — no channel crates; the retry leg coordinates via `Notify` + waker slabs.
- [ADR-0004](0004-sans-io-protocol-core.md) — `magnetar-proto` zero-I/O; the attempt counter + terminal markers are plain struct fields.
- [ADR-0011](0011-clock-injection-sans-io.md) — injected clocks; the backoff sleeps route through them.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime five-layer test + 1:1 parity policy.
- [ADR-0028](0028-supervised-reconnect-anti-thrash-policy.md) — the supervised-reconnect lineage this retry sits on.
- [ADR-0038](0038-split-connection-mutex.md) — split connection mutex; the lock order the terminal methods preserve.
- [ADR-0055](0055-bit-flip-survivability-model.md) §1 — `OpOutcome::Terminal` + `fail_all_pending`, the shape `fail_producer_open` / `fail_consumer_subscribe` scope to one handle.
- [ADR-0059](0059-terminal-fast-fail-new-ops.md) — the `no_driver` latch the receive guard reuses to cover supervised give-up.
