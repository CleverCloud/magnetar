# ADR-0060 — Bounded lookup-retry on `SessionLost`

- **Status**: Accepted
- **Date**: 2026-06-09
- **Decider**: Florentin Dubois
- **Tags**: reconnect, resilience, runtime, lookup, sans-io

## Context

`Connection::reset` (magnetar-proto) is the supervised-reconnect boundary: between a transport drop and the new socket's handshake, it fails every pending request — including an in-flight `CommandLookupTopic` — with `OpOutcome::SessionLost`, so the dying session is torn down cleanly before the new one is built (see [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md)'s reset-drains-lookup unit test).

For in-flight **publishes** `reset` does more: it snapshots each pending send into `in_flight_publish_snapshots` and re-issues them after the new handshake via `rebuild_producers`, **without** installing a `SessionLost` outcome on the Send key.
The user's `SendFut` re-polls, finds no outcome, and stays pending until the replayed publish's receipt arrives — transparent at-least-once replay (mirrors Java `ProducerImpl#resendMessages`).
Consumers are likewise re-subscribed by `rebuild_consumers`.

A **lookup** got neither half: it received a `SessionLost` outcome and was **not** re-issued.
Both engines' `lookup_topic` mapped `OpOutcome::Terminal → ClientError::PeerClosed` but routed `OpOutcome::SessionLost` into the catch-all `other =>` arm → `ClientError::Other("unexpected lookup outcome: SessionLost…")`.
Because `lookup_topic` backs **both** `open_producer` and `subscribe` (Pulsar requires a `CommandLookupTopic` round-trip before either), a production caller subscribing or opening a producer **during** a supervised reconnect could see that `Other` leak — even though the engine was about to transparently replay its producers and re-subscribe its consumers.
This was the last engine residual from the [ADR-0055](0055-bit-flip-survivability-model.md) survivability work (docs/follow-ups.md §4.1).

The Java client does not have this asymmetry: `BinaryProtoLookupService` re-drives the pending lookup future against the fresh connection after a reset.

## Decision

The fix lives **engine-side**, in `lookup_topic`, mirrored 1:1 across both engines (ADR-0024).
`reset`'s `SessionLost` emission is **unchanged** — it is load-bearing for the publish-replay carve-out and for tearing the session down.
The retry re-issues the _request_; it does not alter the outcome semantics.

### A bounded retry loop on `SessionLost`

`lookup_topic` issues the `CommandLookupTopic`, awaits the outcome, and:

1. On `OpOutcome::SessionLost`, it parks in a **wake-or-terminal** manner via `ConnectionShared::await_reconnect_or_terminal()` — a park on the `driver_waker` (which both engines pulse via `notify_waiters()` on every state transition, post-`CommandConnected` included), re-checking the proto state on each wake:
   - **`Reconnected`** (`is_connected()` again) → re-issue the lookup against the fresh session.
   - **`Terminal`** (`is_closed()` **AND** the runtime `no_driver` latch is set) → short-circuit to `ClientError::PeerClosed`.
2. The re-issue is bounded by a new named const `MAX_LOOKUP_SESSION_REISSUES` (magnetar-proto `lookup`, next to `MAX_LOOKUP_REDIRECTS` for a single source of truth).
3. A re-issue counts against the bound **only when a lookup was actually submitted against a connected session** — spurious driver wakes or repeated `SessionLost` within the same not-yet-reconnected window do **not** burn the budget without a real broker round-trip.

The park is on **readiness, not a timer**: there is no host-clock / virtual-clock read in the retry path ([ADR-0011](0011-clock-injection-sans-io.md)).
The `Notified` future is created and `enable()`d **before** the state re-check, so a transition that races between the check and the await is not lost (no bare wake proceeds to re-issue while the connection is still not-connected).

### Why the terminal short-circuit composes with ADR-0059

[ADR-0059](0056-terminal-fast-fail-new-ops.md) §5.1 added the runtime `no_driver` latch — set on the plain driver's terminal exit and the supervisor give-up path.
The `Terminal` branch reuses exactly that latch: a `SessionLost` that lands while the supervisor has already given up (no driver left to reconnect) surfaces a clean `PeerClosed` instead of re-hanging on the readiness signal or spinning to the re-issue bound.
The `Reconnected` / `Terminal` decision lives in `await_reconnect_or_terminal`; the two conditions of the terminal branch (`is_closed()` **and** `no_driver`) are the same two-condition gate ADR-0059's `fail_if_no_driver()` uses, so a recoverable supervised connection in its transient `Failed` window is never wrongly terminalized.

### Engine symmetry

The mirror is **behavioral**, not textual: tokio's `lookup_topic` is a trait method returning `LookupTarget`; moonpool's is a free `pub async fn` returning the raw `LookupOutcome`.
Both express the bounded loop against their own engine's `ConnectionShared::await_reconnect_or_terminal` + `LookupReissueReadiness` enum.
A top-level `OpOutcome::SessionLost` is **not** an `OpOutcome::LookupResponse`, so on the moonpool side it reaches the `SessionLost` arm before the `Ok(other)` `LookupResponse` wrapping — confirmed identical to tokio and pinned by the differential equivalence test so the two engines cannot silently diverge.

### Public behavior change

A transient `SessionLost` on a lookup behind `subscribe` / `open_producer` during a supervised reconnect now resolves **transparently** (the open/subscribe succeeds on the re-issued lookup) instead of surfacing `ClientError::Other("unexpected lookup outcome: SessionLost…")`.
A **terminal** `SessionLost` (supervisor gave up) surfaces `ClientError::PeerClosed` — the same terminal category as an in-flight terminal drop — rather than `Other`.
No previously-`Ok` path changes; only the former `Other` leak is reclassified.

## Consequences

- `subscribe` / `open_producer` issued mid-reconnect recover transparently, closing the last engine residual from the ADR-0055 survivability work and matching Java's lookup-after-reset.
- A persistently flapping connection cannot spin a `lookup_topic` call forever: the loop is bounded by `MAX_LOOKUP_SESSION_REISSUES` re-issues (the ceiling — a single transient reconnect costs at most one re-issue) and short-circuits to `PeerClosed` once the supervisor gives up.
- `magnetar-proto` stays zero-I/O (ADR-0004): the only proto-side addition is a `const`; the loop and the park live in the engines.
- ADR-0024 layers ship in the same commit: a proto unit (`MAX_LOOKUP_SESSION_REISSUES` bound + the re-issue-resolves-on-fresh-session surface + the failed-connection terminal-short-circuit surface), tokio + moonpool integration twins (transient-SessionLost-succeeds, terminal-SessionLost-`PeerClosed`, flap-is-bounded — kept tokio↔moonpool 1:1), a `magnetar-differential` readiness-equivalence test, and an e2e (`e2e_reconnect.rs` extended with a subscribe/open-during-reconnect assertion, no `#[ignore]` / no feature gate per ADR-0046).

## References

- [ADR-0003](0003-no-channels-rule.md) — no channel crates; the readiness park is a `Notify` + atomic, not a channel.
- [ADR-0004](0004-sans-io-protocol-core.md) — `magnetar-proto` zero-I/O; the bound is a plain `const`.
- [ADR-0011](0011-clock-injection-sans-io.md) — clock injection; the retry parks on readiness, reading no clock.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime four-layer test + 1:1 parity policy.
- [ADR-0038](0038-split-connection-mutex.md) — the transient-`Failed` supervised-reconnect window the terminal branch must not regress.
- [ADR-0055](0055-bit-flip-survivability-model.md) — the survivability work that surfaced this residual.
- [ADR-0059](0056-terminal-fast-fail-new-ops.md) — the `no_driver` latch the terminal short-circuit composes with.
