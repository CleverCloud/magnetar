# ADR-0134 — Make a user close sticky in the connection state machine

- **Status**: Accepted
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: connection, supervisor, close, state-machine, tokio, moonpool, sans-io
- **Amends**: [ADR-0083](0083-bounded-cancellable-driver-write.md) and [ADR-0119](0119-use-tokio-time-for-driver-write-deadlines.md)

## Context

`Connection::close()` records the application's intent only as the handshake state `Closing`.
The supervised driver loop has one stop signal, `Connection::is_user_closed()`, true for `Closing` and `Closed` and false for `Failed`; `Failed` is the transport-drop state the supervisor exists to redial (ADR-0059).
Everything the loop concludes from "the user closed" is therefore only as durable as that one state, and three writers could overwrite it.

- **A transport failure inside the close window.**
  The driver keeps writing while bytes are queued, so a close issued while a publish is parked mid-write waits for it, and a read error, an EOF, a decode error or the write deadline of ADR-0083 can fire meanwhile.
  Every one of those arms calls `Connection::mark_disconnected()`, which set `Failed` unconditionally, even from `Closing`.
  The supervisor then read a transport drop, backed off, redialled, called `reset()` and re-attached every producer and consumer, replaying their in-flight publishes; `Client::close()` joins the driver and never returned.
- **A redial that finishes its dial after the close.**
  A dial that completed after the supervisor's last pre-dial check reached `reset()`, which snaps any state to `Uninitialized`.
  [ADR-0052](0052-initial-connect-timeout-retry.md) now closes that window in the runtimes: both supervised loops re-check `is_user_closed()` under the same lock acquisition as the `reset()` that follows the dial.
- **The broker's own handshake frames.**
  A `CommandConnected` that arrives after the close set `Connected`; a `CommandAuthChallenge` set `AuthChallenging`, which `submit_auth_response` answers by moving to `Connected`.
  Both turn a closed connection back into an open one.

The default `max_attempts` is `None`, so nothing else stops the loop: the result is a hung `close().await` and a live session the application believes it ended.
ADR-0083 and ADR-0119 describe the write-deadline expiry as routing through `mark_disconnected()` "so the supervisor redials unchanged"; that stays true for every connection the user has not closed.
ADR-0083 also records that it touches no `magnetar-proto` state machine, which is why it ships without a layer (a) test; this decision does touch it.

## Decision

A user close is **sticky**: once the state is `Closing` or `Closed`, only `reset()` can leave it, and a driver never calls `reset()` on a closed connection.

- **One entry for peer- and transport-caused transitions.**
  `Connection::advance_handshake_state` applies a transition and returns whether it did; it refuses (and logs the two state names at `debug!`, ADR-0054) while `is_user_closed()`.
  `mark_disconnected()`, the `CommandConnected` arm and the `CommandAuthChallenge` arm go through it.
  `mark_disconnected()` on a closed connection records nothing: the disconnect timestamp is the one `close()` already took.
  A refused `CommandConnected` or `CommandAuthChallenge` is dropped whole (no `Connected` / `AuthChallenge` event, no broker-limit update): the handshake it answers is one nobody waits for.
- **`reset()` is unchanged.**
  It still snaps to `Uninitialized`; making it refuse on a closed connection would turn the supervisor's `begin_handshake()` into a protocol error instead of a clean stop, and the post-dial check of ADR-0052 already prevents the call.
- **No new public API, no wire change.**
  `is_closed()`, `is_user_closed()` and `is_terminally_closed()` keep their meaning; only the set of events that can falsify them shrinks.

## Consequences

- `Client::close()` returns once the in-flight write ends, whatever transport failure races it, and a closed client never opens a second session.
- A supervised connection whose user closed during a stalled write now ends at the write deadline instead of redialling: the deadline still bounds the close, as ADR-0083 intends.
- An unsupervised connection is unchanged: it never redialled, and `is_terminally_closed()` was already true for it in both states.
- A `CommandConnected` or `CommandAuthChallenge` that races a close is silently dropped; a client that closes during its initial handshake still observes the close, not the handshake.
- Cost: one branch in the proto state machine.
- Incompatible with: a driver that calls `reset()` without first checking `is_user_closed()`; the supervised loops are the only production callers, and both check it after the dial (ADR-0052).

### ADR-0024 test layers

(a) `magnetar-proto/tests/user_close_sticky.rs` (transport drop and late `CommandConnected` after `close()`) and `conn::conn_state_tests::auth_challenge_after_close_keeps_the_user_close`; (b) `magnetar-runtime-tokio/tests/close_during_stalled_write.rs`; (c) the moonpool twin, `driver_write_deadline.rs::close_during_stalled_write_is_not_redialled_sim`; (d) `magnetar-differential/tests/user_close_stalled_write_equivalence.rs` (close inside a stalled write).
A `CommandConnected` or `CommandAuthChallenge` after a close is covered at layer (a) and by `magnetar-differential/tests/user_close_sticky_contract.rs`, which feeds the frames to the shared `Connection`: both runtime drivers stop reading as soon as the close has nothing left to write, so no deterministic runtime scenario delivers one while `Closing`.
End-to-end: `crates/magnetar/tests/e2e_user_close_sticky.rs` closes a supervised client in front of a real broker while a TCP proxy that no longer reads what the client writes keeps its write stalled; `close()` must return, and the client must not dial again.

## References

- `crates/magnetar-proto/src/conn.rs` — `Connection::advance_handshake_state`, `mark_disconnected`, the `Connected` and `AuthChallenge` arms of `handle_frame`.
- [ADR-0052](0052-initial-connect-timeout-retry.md) — the post-dial `is_user_closed()` check in both `supervised_driver_loop`s, which keeps a redial from calling `reset()` on a closed connection.
- [ADR-0083](0083-bounded-cancellable-driver-write.md), [ADR-0119](0119-use-tokio-time-for-driver-write-deadlines.md) — the write deadline whose `mark_disconnected()` route this decision qualifies.
- [ADR-0059](0059-terminal-fast-fail-new-ops.md) — `is_user_closed()` versus the transient `Failed` window.
- [ADR-0054](0054-logging-policy.md) — handshake transitions are logged by state name only.
