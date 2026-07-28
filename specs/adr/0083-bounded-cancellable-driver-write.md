# ADR-0083 — Bounded, cancellable, deadline-guarded driver write as a `select!` arm

- **Status**: Accepted
- **Date**: 2026-07-27
- **Decider**: Florentin Dubois
- **Tags**: runtime, driver-loop, reliability, determinism

## Context

Issue #370: the driver's outbound write ran **unconditionally at the top of every loop iteration**, before `tokio::select!` / `moonpool_core::select!` ever polled.
That shape is exactly what ADR-0070 (issue #303) and ADR-0074 (issue #319) built on: ADR-0070's read-first reorder reasoned that "the outbound path is not starved by giving reads priority, because `poll_transmit` + `write_all` already run at the TOP of every loop iteration regardless of which arm wins"; ADR-0074 bounded that top-of-loop write to `DRIVER_WRITE_BUDGET_BYTES` (256 KiB) per turn and added a fixed continuation arm that fires whenever bytes remain, so the read arm still got a look-in between large bursts.

Both ADRs' safety arguments depend on the write completing (or at least yielding control back to the loop) in bounded time.
Neither considered a peer that **accepts the connection and then simply stops draining its receive window** — no error, no close, just silence in one direction.
Against such a peer, the write's single `AsyncWrite::write_all` (tokio) / `Transport::write_all` (moonpool) call blocks forever inside that unconditional top-of-loop step.
Because the write sits ahead of the `select!`, it blocks the ENTIRE loop — not just the write path.
The read arm, the `driver_waker` arm, and (critically) the timer arm that drives `Connection::handle_timeout` never run again.
`handle_timeout` is the sans-io state machine's sole entry point for the keepalive watchdog (ADR-0058), the `send_timeout` sweep, and the `ack_response_timeout` backstop — grepped as the only non-test call site in both engines.
With it starved, `mark_disconnected()` is never reached and `Connection::is_connected()` keeps reporting `true` on a connection that is functionally dead.
No existing test caught this: the seven currently-open moonpool seed failures in this batch are unrelated (confirmed by a prior repro-focused instrumentation pass that found the moonpool driver's writes completing in microseconds against those seeds, with the ~30s keepalive firing on schedule) — issue #370 is a real, independently-discovered latent defect, not the cause of any observed CI flake.

`keepalive_interval` cannot substitute as the write-side deadline: it fires off `last` (a **read**-side liveness timestamp).
A peer that keeps answering `PING` with `PONG` while silently refusing to drain our writes — a real, if unusual, failure mode (a stuck receive-side application behind a still-healthy TCP stack) — would never trip it.

### Cancellation-safety sub-problem

Making the write a genuine `select!` arm means it can be **dropped mid-poll** whenever another arm wins a round — routine, not exceptional, once it races against read/waker/timer.
Two independent pre-existing write paths were not safe to cancel:

- Tokio's `PendingDriverWrite::write_budgeted` looped `stream.write_all(&front[off..off+n]).await?` per chunk, advancing `front_offset` only **after** the whole chunk's `write_all` resolved.
  Cancelling mid-`write_all` loses track of bytes the kernel may already have accepted; a resumed write would re-send them — **duplication**, not a clean failure.
- Moonpool's `PendingDriverWrite::pop_budgeted` eagerly **detached** an entire budget's worth of segments out of `segments`/`front_offset` into an owned `Vec<Bytes>` before any I/O was attempted.
  Cancelling a `write_budgeted` call mid-iteration over that vector **drops** chunks that were popped-and-detached but never actually sent — silently, not even duplicated.
- Moonpool's `Transport::write_all` Tls arm ran `adapter.push_plaintext` → `adapter.step()` → `adapter.take_encrypted_outbound()` (which **drains** the adapter's own ciphertext buffer) synchronously, then awaited the single cancel-exposed `stream.write_all(&ciphertext)`.
  A cancelled write after `take_encrypted_outbound()` had already run loses the ciphertext outright — it exists nowhere but the local `ciphertext: Bytes` the dropped future was holding.

Both remedies (bound the write; make it cancellable) had to land together — bounding without fixing cancellation-safety would trade an unbounded hang for silent byte loss/duplication under contention, which is a worse defect.

## Decision

### 1. The write becomes its own `select!` arm — third in order, gated by `write_has_work`

Both engines' `driver_loop_inner` grow a fourth arm (was three: read, waker, timer; the old bounded-write continuation arm from ADR-0074 is replaced, not added to):

```
biased;
r = read_half.read_buf(&mut read_buf) => { … }             // 1st — ADR-0070
() = shared.driver_waker.notified() => { … }                // 2nd — ADR-0070
write_result = write_one_budget(…), if write_has_work => { … }  // 3rd — NEW
() = sleep_or_pending(&time, sleep_dur) => { … }             // 4th (was 3rd)
```

`write_has_work` is `!pending_write.is_empty() || write_half.has_pending_ciphertext()` (the second disjunct exists only on moonpool's `Tls` arm — see §3).
The `if` guard means the arm is not even polled when there is nothing to send, so an idle connection never pays for an extra empty poll.
`biased;` is retained on both engines (moonpool's bit-for-bit reproducibility depends on it: a non-biased `select!` picks arms via an uncontrolled thread-local RNG); the read arm stays first, preserving ADR-0070's issue #303 fix exactly.
`DRIVER_WRITE_BUDGET_BYTES` (256 KiB, from ADR-0074) is unchanged as the per-arm-win batch cap.

Both ADR-0070 and ADR-0074's central premise — "the write is not part of the `select!`, so ordering the OTHER arms cannot starve it" — is exactly what issue #370 shows is false once a peer stops draining.
Making the write an arm removes that premise instead of patching around it.

### 2. Read/write split — tokio trivial, moonpool restructured

Two independent mutable borrows of the socket are needed once read and write are separate `select!` arms.

- **Tokio**: `driver_loop_inner<S>` already takes `socket: &mut S` directly (no wrapper type).
  `&mut S` is itself `AsyncRead + AsyncWrite` when `S` is (tokio's blanket impls for `&mut T`), so `let (mut read_half, mut write_half) = tokio::io::split(socket);` at loop entry is suffient — no new type, no `Arc`, no lock.
  `tokio_rustls::TlsStream` is already one opaque `AsyncRead + AsyncWrite` value on this engine, with no user-visible internal buffer the driver needs to share across the split, so TLS needs nothing extra here.
- **Moonpool**: `Transport<P>` is a closed enum owning the stream directly; splitting it needs restructuring.
  `Transport::into_split(self) -> (TransportReadHalf<P>, TransportWriteHalf<P>)` uses `futures::io::AsyncReadExt::split` (a `BiLock`-backed, by-value split — **not** `tokio::io::split`, which targets tokio's own `AsyncRead`/`AsyncWrite` traits that this engine's provider streams, `Compat<tokio::net::TcpStream>` / `moonpool_sim`'s `SimTcpStream`, do not implement).
  The `Plain` arm needs nothing more: `stream` is the only field and is direction-exclusive once split.
  The `Tls` arm is different — `RustlsByteAdapter::step()` is inherently bidirectional (one call drains inbound ciphertext into plaintext AND drains queued outbound plaintext into ciphertext) — so the two TLS halves share one `Arc<parking_lot::Mutex<TlsShared>>` (`TlsShared` bundles the adapter with the new resumable ciphertext queue, §3).
  The mutex is never held across an `.await` (`step()` is fully synchronous), so this is the same "never park while holding a `parking_lot` guard" discipline CLAUDE.md invariant #1 already requires; `is_send::<TransportReadHalf<SimProviders>>()` / `is_send::<TransportWriteHalf<SimProviders>>()` compile-time checks confirm the split stays `Send` for the sim provider.
  `Transport` itself is unchanged for its pre-split callers (connect/handshake, and the standalone connect-bootstrap loop in `lib.rs`) — it now embeds the same `Arc<Mutex<TlsShared>>` internally so `into_split` is a cheap `Arc::clone`, not a state migration.

This is a deliberate, accepted asymmetry between the two engines, not an oversight: ADR-0070's "Alternatives considered" already flagged "the rustls TLS stream cannot `into_split` cleanly" as the reason a full two-task split was deferred — that reasoning is about tokio's `tokio_rustls::TlsStream` specifically and does not apply symmetrically to moonpool's home-grown `Transport`, which this ADR restructures instead.
This is **not** the full two-task reader/writer split ADR-0070 deferred (a dedicated reader task, à la `pulsar-client-go`): both halves stay in the same task, spliced back together every loop iteration by the same `select!` — no `tokio::spawn` boundary crosses the connection, and the supervised-reconnect coordination problem ADR-0070 flagged for the two-task design never arises.

### 3. Cancellation safety (prerequisite, logically a separate change from §1/§2)

- **Tokio** `PendingDriverWrite::write_budgeted`: replaced the `write_all`-per-chunk loop with a loop over the **single-poll** `AsyncWriteExt::write(...)`, committing `self.front_offset += n` synchronously right after each `Ready(n)`, before the next `.await`.
  `write()`'s contract makes this sound: `Pending` means nothing was consumed (nothing to commit); `Ready(n)` commits in the same synchronous stretch of code the poll returned in.
  `Ok(0)` on a non-empty buffer is promoted to `io::ErrorKind::WriteZero`.
- **Moonpool** `PendingDriverWrite`: `pop_budgeted` is deleted from the production path entirely (not demoted — no test needed it kept).
  `write_budgeted` now slices ONE chunk of at most `min(remaining, front_available)` per loop iteration **without** advancing `front_offset`, awaits `TransportWriteHalf::write_some` (moonpool's single-poll primitive, mirroring tokio's `write()`), and only then commits `front_offset` / pops the front segment.
- **Moonpool `Tls`**: a new resumable `pending_ciphertext: BytesMut` + `pending_ciphertext_offset: usize` pair (bundled into `TlsShared`) sits between the adapter and the wire.
  `write_some`/`write_all` drain `pending_ciphertext` FIRST (a single low-level write, offset committed synchronously after `Ready(n)`) and only push new plaintext through the adapter when it is fully drained; the freshly-`take_encrypted_outbound()`'d bytes are captured into `pending_ciphertext` synchronously (`push_plaintext` → `step` → `absorb_adapter_output`, no `.await` in between) before any wire write is attempted, so that capture step can never itself be cancelled mid-way — either the whole chunk is durably represented in `pending_ciphertext`, or the function was never entered.

A gap the naive version of this design would have reopened: `RustlsByteAdapter::step()` runs unconditionally on every `read_buf` call too, and can produce protocol-mandated outbound bytes (a TLS 1.3 `KeyUpdate` acknowledgement, a `close_notify` echo) as a side effect of decrypting inbound data — completely independent of whether the application has anything to write.
Pre-this-ADR that was safe by accident (the top-of-loop write ran every iteration regardless).
Post-split, if the read half could only leave those bytes sitting in the adapter's own buffer, they would strand on an otherwise write-idle connection — a narrower recurrence of the exact bug this ADR fixes.
Closed structurally: `TlsShared::absorb_adapter_output()` — called by BOTH halves after their own `step()` — moves anything the adapter just queued into the shared `pending_ciphertext`.
The read half only ever **appends**; it never touches the socket.
`TransportWriteHalf::has_pending_ciphertext()` is part of `write_has_work`, so a read-triggered append makes the write arm fire on the very next loop iteration with no extra wake plumbing — the loop already recomputes `write_has_work` fresh at the top of every iteration.

### 4. The deadline

Source: `Connection::operation_timeout()` — a plain (non-`Option`) `Duration`, defaulting to 30 s, never actually unset at the `Connection` level (the façade's `ClientBuilder::operation_timeout: Option<Duration>` only conditionally overrides the proto default).
**Not** `keepalive_interval` — see Context.

Re-arm granularity: **per logical write, not per `select!` round**.
The write arm's future expression (`write_one_budget(...)`) is reconstructed fresh every time `select!` is written in source, i.e. every outer-loop iteration — this was discovered empirically while validating the fix under moonpool-sim: naively wrapping `time.timeout(operation_timeout, …)` freshly inside the arm expression re-arms a full 30 s budget every time ANY other arm (read, waker, or an unrelated timer tick such as the keepalive interval itself) wins a round, so an in-flight stalled write facing routine background traffic on the SAME connection would never accumulate 30 s of real elapsed time toward its own deadline.
Both drivers instead track `write_deadline: Option<Instant>` OUTSIDE the `select!`, armed to `now + operation_timeout` on the transition into `write_has_work`, held fixed while work continues across iterations, and cleared once the queue (and, on moonpool, `pending_ciphertext`) fully drains.
`write_one_budget` computes `remaining = deadline.saturating_duration_since(now)` and races `write_budgeted` + flush against that.
The rejected per-single-`write()`-call alternative (re-arming the deadline before every individual low-level write) was not chosen: it would let a peer that accepts one byte every 29 seconds satisfy the deadline forever while never making meaningful progress — the deadline must bound the whole logical write, not each syscall.

Expiry: `Elapsed` maps to `std::io::Error::new(ErrorKind::TimedOut, "driver write deadline exceeded")`, logged via `warn!(deadline_ms, pending_bytes, "driver write deadline exceeded")` (ADR-0054 — structured fields, not a bare format string), and routed through the **exact same branch** every other write error already takes: `mark_disconnected()` then `return Err(...)`.
No new error path, no new supervisor logic — the existing reconnect machinery redials unchanged.

Moonpool routes the bound through the injected `TimeProvider::timeout` (ADR-0011) — **never** `tokio::time::timeout` and never a host-clock read — so `moonpool-sim` runs stay bit-for-bit reproducible.

`close_after_write`: the pending-and-close check now runs in two places — once at the top of the loop (nothing pending, no TLS residue, close requested → shut down immediately, since the gated write arm would never otherwise fire) and once in the write arm's success body (queue just drained by this win → shut down).

`push_transmit`'s `debug_assert!(self.is_empty())` is untouched: a new transmit is still only pulled once the pending queue drains.

## Consequences

- **Easier**: a peer that silently stops draining our writes is detected and disconnected within one `operation_timeout` window (default 30 s) instead of wedging the connection as permanently "connected" — the supervisor redials exactly as it does for any other write failure.
- **Easier**: the cancellation-safety rewrite is a strict improvement independent of the deadline — `write_budgeted` was already implicitly relying on "the write never gets cancelled" as an invariant nothing enforced; now it is actually true under cancellation.
- **Harder**: moonpool's `Transport` is now visibly asymmetric from tokio's plain `&mut S` split — two more types (`TransportReadHalf`, `TransportWriteHalf`), a `TlsShared` struct, and an `Arc<Mutex<_>>` on the TLS path that tokio's engine has no equivalent of. This is intentional (§2) but is a real ongoing verbatim-mirroring cost between the two engines' `transport`/`driver` modules.
- **Harder / residual risk**: `tokio_rustls::client::TlsStream`'s behaviour when only the read half is polled and a protocol-mandated response (KeyUpdate ack) is pending internally could not be verified from source in this change (tokio-rustls's internal buffering is opaque to this crate, unlike moonpool's home-grown adapter which this ADR gives full control over).
  Narrower risk than moonpool's: the workspace's TLS config uses `with_no_client_auth()` and `KeyUpdate` is server-initiated and rare.
  Flagged as a residual risk, not asserted safe — a future soak test exercising a read-only-idle period on a long-lived TLS tokio connection would close this gap; not done here.
- **Cost**: `DRIVER_WRITE_BUDGET_BYTES` and the deadline both bound the write, so a peer that is genuinely slow but still progressing (rather than fully stalled) could in principle be false-positived if its per-30s progress is smaller than expected under extreme backpressure. This mirrors the existing risk profile of every other `operation_timeout`-bounded operation in the codebase (lookup, producer-open, subscribe) and is not a new class of risk.
- **Incompatible with**: reintroducing an unconditional top-of-loop write (would resurrect issue #370) or dropping `biased;` (would break moonpool determinism, per ADR-0070).

### ADR-0024 layer (a) — explicitly absent

This change touches no `magnetar-proto` state machine: `Connection::operation_timeout()`, `handle_timeout`, `mark_disconnected`, and `is_connected()` are all pre-existing and unmodified.
The defect and the fix are entirely runtime-scheduling (which `select!` arm runs when, and how a socket write is bounded/resumed) — there is no new sans-io concept to unit-test at the proto layer.
Per the issue #303 / ADR-0070 precedent (also a pure scheduling fix, also shipped without a proto unit test), layer (a) is deliberately not manufactured here.
The other four layers ship in full: (b) `magnetar-runtime-tokio/src/driver.rs::tests::stalled_write_is_bounded_by_operation_timeout` (+ the cancellation-safety-focused `write_budgeted_is_cancel_safe_across_a_dropped_await`), (c) `magnetar-runtime-moonpool/tests/driver_write_deadline.rs` (+ `transport::tests::write_some_tls_is_cancel_safe_across_a_dropped_await`), (d) `magnetar-differential/tests/driver_write_deadline_equivalence.rs`, (e) `crates/magnetar/tests/e2e_keepalive_watchdog.rs` (extended — see that file for the stall-phase addition, if landed in the same changeset).

## References

- `crates/magnetar-runtime-tokio/src/driver.rs` — `PendingDriverWrite::write_budgeted`, `write_one_budget`, the `select!`'s write arm, `write_deadline`.
- `crates/magnetar-runtime-moonpool/src/driver.rs` — mirror of the above.
- `crates/magnetar-runtime-moonpool/src/transport.rs` — `Transport::into_split`, `TransportReadHalf`, `TransportWriteHalf`, `TlsShared`, `write_some_tls`, `write_some_plain`.
- `crates/magnetar-runtime-tokio/src/driver.rs::tests::stalled_write_is_bounded_by_operation_timeout` / `::write_budgeted_is_cancel_safe_across_a_dropped_await`.
- `crates/magnetar-runtime-moonpool/tests/driver_write_deadline.rs`, `crates/magnetar-runtime-moonpool/src/transport.rs::tests::write_some_tls_is_cancel_safe_across_a_dropped_await`.
- `crates/magnetar-differential/tests/driver_write_deadline_equivalence.rs`.
- `crates/magnetar/tests/e2e_keepalive_watchdog.rs`.
- [ADR-0070](0070-driver-read-arm-fairness.md) — read-arm-first ordering this change preserves; its "outbound is not starved because the write runs at the top of the loop" premise is the one issue #370 disproves and this ADR replaces. Amended by this ADR.
- [ADR-0074](0074-driver-bounded-write-fairness.md) — `DRIVER_WRITE_BUDGET_BYTES` and the bounded-write-turn shape this ADR keeps; its fixed unconditional continuation arm is replaced by the gated, cancellable, deadline-bound arm described here. Amended by this ADR.
- [ADR-0058](0058-keepalive-watchdog-progress-based.md) — the read-side watchdog `handle_timeout` drives; this ADR is why that timer can now always run.
- [ADR-0011](0011-clock-injection-sans-io.md) — sans-io clock injection; the moonpool deadline routes through the injected `TimeProvider`, never a host clock.
- [ADR-0003](0003-no-channels-rule.md) — the deadline signals expiry via `Result`/`Err`, not a new channel; `driver_waker` (`Notify`) is unchanged.
- [ADR-0038](0038-split-connection-mutex.md) — the `parking_lot::Mutex` lock-ordering discipline `TlsShared`'s mutex follows (never held across `.await`).
- Issue #370 — original report.
