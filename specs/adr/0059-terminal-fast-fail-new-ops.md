# ADR-0059 — Terminal-state fast-fail for NEW operations + no-driver signal

- **Status**: Accepted
- **Date**: 2026-06-09
- **Decider**: Florentin Dubois
- **Tags**: reconnect, resilience, runtime, sans-io

## Context

[ADR-0055](0055-bit-flip-survivability-model.md) §1 added `OpOutcome::Terminal` + `Connection::fail_all_pending(reason)` so that on a **plain** (non-supervised) driver's terminal exit, every op _pending at the drop_ resolves promptly with `ClientError::PeerClosed` instead of hanging.
That contract is explicitly scoped to the **in-flight** ops — the requests, sends, and consumer receive-wakers that were already registered when the peer vanished.

It leaves a symmetric gap for **new** ops issued _after_ the connection is already terminal.
Once the plain driver has exited (or a supervisor has exhausted its reconnect-attempt budget), there is no driver task left to drain the connection's outbound buffer or resolve a pending request.
A `producer.send()` / `subscribe()` / `producer.close()` / `lookup` issued in that window registers a brand-new pending op that nothing will ever resolve — the caller hangs forever, the exact no-progress stall ADR-0055 §1 set out to kill, just on the next op instead of the in-flight one.

Two facts make a naive guard unsound:

- **`is_closed()` alone over-fires.** A **supervised** connection is transiently `Failed` between `mark_disconnected()` (on the drop) and the supervisor's `reset()` → `Uninitialized` (on the next attempt) while it _will_ recover.
  `is_closed()` returns `true` for `Failed`, so gating a new op on `is_closed()` alone would wrongly `PeerClosed` an op issued mid-reconnect and regress transparent reconnect (ADR-0038).
- **`is_user_closed()` cannot tell the two apart.** It excludes `Failed` by design (it gates user-initiated closes — ADR-0055 §1, conn.rs `is_user_closed`), so it returns `false` for _both_ a recoverable-`Failed` and a terminal-`Failed`.
  It cannot distinguish "the supervisor is reconnecting" from "the supervisor gave up".

The distinguishing fact is not in the proto state machine at all: it is **whether a driver task is still alive to make progress**.
That is runtime state, not protocol state.

## Decision

### A new-op contract that extends ADR-0055 §1's in-flight-only scope

A NEW op issued after a genuinely-terminal drop fast-fails **synchronously** with `ClientError::PeerClosed`, on both engines, mirrored 1:1 (ADR-0024).
Three coordinated pieces:

#### 1. Slot-close inside `fail_all_pending` (sans-io)

`Connection::fail_all_pending` (magnetar-proto) flips each producer slot's `closed` flag **inside the existing per-slot lock scope** where it already drains that slot's pending sends — one lock acquisition, no second slot loop.
A post-terminal `ProducerSlot::queue_send` then fast-fails through the **existing** `if self.closed` guard with `ProducerError::Closed`, so the producer hot path (`Producer::send`) never reads the connection-wide mutex.

This preserves the ADR-0038 lock order exactly: `fail_all_pending` already runs under the global connection mutex and takes each per-slot mutex _below_ it; the flag write rides the same downward acquisition.
The slot guard is dropped before any user waker fires.

This stays zero-I/O (ADR-0004): it is a plain `bool` write on a struct already behind the per-slot `parking_lot::Mutex`.

#### 2. A runtime `no_driver` latch (one per engine, 1:1)

Each engine's `ConnectionShared` carries a new `no_driver: AtomicBool`, set `true` on — and only on — a genuinely-terminal exit, paired 1:1 with the `fail_all_pending` call already at each site:

- the **plain** driver's terminal-exit path (`spawn`), and
- the **supervisor give-up** return path (`spawn_supervised`, after `supervised_driver_loop` returns — which it does only on a user close or an exhausted attempt budget, never on a per-attempt reconnect).

It is set **after** `fail_all_pending` so the slot `closed` flags and terminal outcomes are already in place when a fresh op observes the latch.
An `AtomicBool` (not a channel) is the right primitive for this one-way latch (ADR-0003).
This is runtime state by necessity — the proto state machine cannot tell a recoverable-`Failed` from a terminal-`Failed`, but the driver that exits _knows_ it is the last one.

#### 3. Entry-point guards (`is_closed()` AND `no_driver`)

The engine entry points that _register a pending op_ — `open_producer` / `subscribe` / `lookup_topic` / `producer.close()` — run a synchronous `fail_if_no_driver()` check **before** registering, returning `ClientError::PeerClosed` when **both** `no_driver` is latched **and** the connection `is_closed()`.

Both conditions are load-bearing:

- gating on `is_closed()` alone would `PeerClosed` a recoverable supervised connection in its transient `Failed` window (regressing transparent reconnect);
- gating on `no_driver` alone would be unsound on a freshly-constructed connection whose driver has not yet started.

Together they pin exactly the "doomed new op" case.
The producer-send path maps the resulting `ProducerError::Closed` to `ClientError::PeerClosed` (the terminal-outcome category) **only when `no_driver` is latched** — otherwise a `Closed` rejection is a genuine protocol-state error and keeps the engine's generic protocol mapping.

### Why `PeerClosed`, not `Closed`

`ClientError::Closed` is the _user-requested graceful close_ outcome (a `Closed { reason: None }` event — ADR-0055 §1).
A terminal drop is an _involuntary_ peer/transport loss, so it surfaces as `PeerClosed`, identical to the in-flight contract.
A caller cannot otherwise tell "I closed it" from "it died under me".

## Consequences

- A plain connection that has gone terminal fast-fails a fresh `send` / `subscribe` / `close` / `lookup` with `PeerClosed` synchronously, completing the no-hang guarantee ADR-0055 §1 started — the in-flight op _and_ the next op both fail fast, never stall.
- A supervised connection mid-reconnect (transiently `Failed`, `no_driver == false`) is **never** `PeerClosed` by these guards; transparent reconnect is preserved (ADR-0038). This is the regression the guards' two-condition gate exists to avoid.
- The producer hot path takes only the per-slot mutex; the new fast-fail rides the existing `if self.closed` guard, adding no connection-mutex read on `Producer::send`.
- ADR-0024 layers: this is a behavioral `magnetar-proto` + runtime change, so it ships with a proto unit test (slot-close + post-terminal `queue_send` rejection + lock-order witness), both runtime integration tests (the new-op-after-terminal case + a supervised-mid-reconnect regression test, kept tokio↔moonpool 1:1), a `magnetar-differential` equivalence test (new-op terminal outcome identical across engines), and an e2e (`e2e_terminal_exit.rs` extended; the prior "out of scope" scope-note is removed).

## References

- [ADR-0003](0003-no-channels-rule.md) — no channel crates; `AtomicBool` is the latch primitive.
- [ADR-0004](0004-sans-io-protocol-core.md) — `magnetar-proto` zero-I/O; the slot-close is a plain `bool` write.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime four-layer test + 1:1 parity policy.
- [ADR-0038](0038-split-connection-mutex.md) — split connection mutex; lock-ordering (global → per-slot) the slot-close preserves; the transient-`Failed` reconnect window the guards must not regress.
- [ADR-0055](0055-bit-flip-survivability-model.md) §1 — the in-flight-only terminal fail-fast this ADR extends to new ops.
