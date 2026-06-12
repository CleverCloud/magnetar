# ADR-0058 — Progress-based keepalive watchdog: refresh on decoded frames, escalate on the second missed interval

- **Status**: Accepted
- **Date**: 2026-06-12
- **Decider**: Florentin Dubois
- **Tags**: proto, keepalive, resilience, reconnect, chaos, moonpool

## Context

Pulsar command frames carry **no checksum** — only message _payloads_ carry CRC32C (workspace invariant 4).
A single-bit flip on the un-checksummed outer `total_size` length prefix (`crates/magnetar-proto/src/frame.rs:301` `peek_full_frame_len`) cannot occur over production TCP, but moonpool-sim injects it by design ([ADR-0055](0055-bit-flip-survivability-model.md)).
ADR-0055 modelled two outcomes of such a flip: a CRC-bearing payload frame (verify-or-drop) and a command frame whose flip makes the byte stream a **fatal** `Frame(Decode(..))`.

There is a **third** outcome ADR-0055 did not cover: a flip that lands on the length prefix and yields a _plausible_ length (`0 < N <= MAX_FRAME_SIZE`) whose promised bytes never arrive.
`peek_full_frame_len` then returns `Ok(None)` — "incomplete, wait for more bytes" — **forever**.
No error is ever raised; the connection simply parks waiting for a frame that the corrupted length will never complete.

Two `magnetar-proto` behaviours turned that park into a permanent wedge:

1. **Keepalive baseline refreshed per raw chunk.** `handle_bytes` / `handle_bytes_owned` set `self.last_activity = Some(now)` on _every inbound chunk_, before any framing.
   A desynced-but-_chatty_ socket — one that keeps dribbling bytes that never frame — therefore reset the keepalive deadline on every read, so `poll_timeout` kept sliding the deadline forward and `handle_timeout` never judged the connection idle.
   The keepalive watchdog never fired.
2. **`handle_timeout` only ever re-pinged.** When the keepalive interval did elapse, `handle_timeout` (`crates/magnetar-proto/src/conn.rs`) encoded a `CommandPing`, reset `last_activity = now`, and returned.
   It never escalated.
   Even a fully silent half-open socket (no chatter at all) was pinged forever, never failed.

The result is a connection that is alive on the wire but dead to the application, never reconnecting — reported as moonpool seed failures `0xa643e7ad4c47c32e` (#187) and `0x2c60abc681532cd6` (#221).

Alternatives considered:

- **Make `peek_full_frame_len` reject a length that cannot be satisfied within a deadline.**
  Rejected: the framing layer is sans-io and stateless per call; it has no clock and no notion of "how long have we waited".
  Liveness is a connection-level concern, which is exactly what the keepalive watchdog already owns.
- **Cap the wait with a separate "incomplete-frame" timer.**
  Rejected: it duplicates the keepalive deadline machinery with a second timer for the same liveness question.
  The keepalive watchdog is the single existing liveness signal; the fix is to make it _correct_, not to add a parallel one.
- **Drop the connection on the first missed keepalive interval.**
  Rejected: one missed interval is the _normal_ trigger to send a ping; a healthy-but-quiet link must be pinged, not failed.
  Java's `ClientCnx#handleKeepAliveTimeout` likewise fails only after a ping goes unanswered, not on the first idle tick.

## Decision

Make the keepalive watchdog **progress-based** and give it an **escalation step**, both in `magnetar-proto`:

- **Refresh `last_activity` per _decoded frame_, never per raw chunk.**
  The single refresh site moves into `handle_bytes_decode_loop`, immediately after a complete frame is carved off the inbound buffer (`self.inbound.split_to(frame_len)`).
  It covers every decode outcome — a v4 frame, a scalable command, and even a CRC-mismatch drop — because all three consumed a real, fully-framed unit off the stream.
  `handle_bytes` / `handle_bytes_owned` no longer touch `last_activity`.
  A desynced-but-chatty socket whose bytes never satisfy the announced `total_size` therefore cannot keep the watchdog baseline fresh.

- **Add `keepalive_ping_outstanding: bool`.**
  `handle_timeout`, on a due keepalive interval while connected:
  - if a ping is already outstanding (the previous interval pinged and **no decoded frame** has since cleared it), call `mark_disconnected()` → `HandshakeState::Failed`;
  - otherwise emit the `CommandPing`, set `keepalive_ping_outstanding = true`, and refresh `last_activity = now`.
    The flag is cleared by the same per-decoded-frame progress update above — _any_ decoded inbound frame proves the peer is alive, so the reset is not pong-specific.
    `reset()` clears the flag unconditionally so a fresh session starts with a clean watchdog (the `connection.reset.delay` buggify only ages `last_activity`, it must not carry a stale outstanding-ping flag across a reconnect).

The driver already treats `Failed` as `should_close` (`crates/magnetar-runtime-{tokio,moonpool}/src/driver.rs`), so a **supervised** client redials and re-handshakes; a **plain** client exits its driver and `fail_all_pending` surfaces a terminal error ([ADR-0055](0055-bit-flip-survivability-model.md) §1).
No new driver code is required — the fix is entirely in the sans-io state machine, and the existing `should_close` path carries it.

## Consequences

- A chatty desync (or any unparseable-but-plausible length prefix) is now bounded: the watchdog fails the connection on the second consecutive unanswered keepalive interval instead of parking forever.
  This is the liveness complement to ADR-0055's integrity story — together they cover all three flip outcomes (drop, fatal decode, and now the silent-incomplete wedge).
- A genuinely silent half-open socket (no chatter) is also failed now, where before it was pinged forever — a general liveness win independent of chaos.
- A healthy-but-quiet link is unaffected: one idle interval pings, an answer (any decoded frame) clears the flag, and the watchdog re-arms; it is never failed for being quiet.
- The escalation rides the **existing** `Failed` → `should_close` → supervised-reconnect path, so it composes with transparent producer/consumer rebuild ([ADR-0038](0038-split-connection-mutex.md)) at no extra cost.
- ADR-0024 layers: this is a behavioral `magnetar-proto` change, so it ships in the same changeset with a proto unit test, both runtime integration tests (kept tokio↔moonpool 1:1), a `magnetar-differential` equivalence test pinning the shared watchdog decision, and an e2e (`e2e_keepalive_watchdog.rs`) that black-holes a live connection and asserts the supervised client recovers.

## References

- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime test + coverage policy.
- [ADR-0038](0038-split-connection-mutex.md) — split connection mutex; reconnect replay + `Failed`/`should_close` plumbing.
- [ADR-0055](0055-bit-flip-survivability-model.md) — bit-flip survivability; this ADR closes the silent-incomplete-length gap it left open.
- `crates/magnetar-proto/src/conn.rs` — the `last_activity` per-decoded-frame refresh, the `keepalive_ping_outstanding` field, and the `handle_timeout` escalation.
- `crates/magnetar-proto/src/frame.rs:301` — `peek_full_frame_len`, which returns `Incomplete` forever on a plausible-but-unsatisfied length.
- `crates/magnetar-runtime-{tokio,moonpool}/tests/keepalive_watchdog.rs`, `crates/magnetar-differential/tests/keepalive_watchdog_equivalence.rs`, `crates/magnetar/tests/e2e_keepalive_watchdog.rs` — the ADR-0024 test layers.
