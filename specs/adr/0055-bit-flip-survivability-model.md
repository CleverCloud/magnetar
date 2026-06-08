# ADR-0055 — Bit-flip survivability: terminal fail-fast + supervised chaos tests

- **Status**: Accepted
- **Date**: 2026-06-08
- **Decider**: Florentin Dubois
- **Tags**: testing, chaos, moonpool, resilience, reconnect

## Context

`moonpool-sim` injects FoundationDB-style **bit-flip corruption** on the bytes it delivers to every connection that has not been explicitly marked _stable_ (`SimWorld::mark_connection_stable`, FDB `sim2.actor.cpp` `stableConnection` parity).
The knob is `ChaosConfiguration::bit_flip_probability`, default `0.0001` ("matches FDB's `BUGGIFY_WITH_PROB(0.0001)`"), applied in `handle_data_delivery` via `maybe_corrupt_data`.
`SimulationBuilder::new()` — used by the whole `sim_chaos` suite — selects `NetworkConfiguration::default()`, so **bit-flip is on by default** and magnetar never marks its sim connections stable.

Pulsar command frames carry **no checksum** — only message _payloads_ carry CRC32C (workspace invariant 4).
In production TCP guarantees command-frame integrity, so a single-bit flip on a command frame cannot occur; the sim models it anyway.
A flip has two failure modes:

1. **Payload frame** (CRC-bearing) → CRC32C verify-or-drop → `ConnectionEvent::ChecksumMismatch` → the frame is dropped.
   Recoverable _iff_ the un-acked message is redelivered.
2. **Command frame / length prefix** → fatal `Frame(Decode(..))` error → the byte stream is unparseable from that point on.

On a **plain** (non-supervised) connection the fatal decode killed the driver task and left every parked `subscribe()` / `send()` / `receive()` future waiting forever — a moonpool no-progress stall.
This was a latent landmine: the `sim_chaos` suite has always run with bit-flip on and passed by luck, because a flip rarely lands on a command frame.
The `feat/logging` re-attach fix (`a02f401`) changed the outbound write schedule, which shifted moonpool's deterministic RNG so that for the two ADR-0038 anchor seeds (`0x56201ccaba82dbc1` / `0xdc638c565234d23f`) a flip now lands on `subscribe[1]`'s `CommandSuccess` — turning `seed-replay` red.

The root cause is therefore **neither** `a02f401`'s logic **nor** a moonpool delivery bug.
It is intended chaos exposing two real gaps: a plain-connection robustness gap (a terminal drop hangs pending ops instead of failing them) and the `sim_chaos` brokers' lack of cross-reconnect state (a reconnect loses the ledger, so recovery cannot redeliver).

Alternatives considered:

- **Disable bit-flip for magnetar's sim** (mark connections stable, or `bit_flip_probability = 0`).
  Rejected: it discards a real fuzzing axis to paper over the symptom.
  The better fix is to make the driver survive corruption the same way it survives any other drop.
- **Make moonpool's bit-flip opt-in** (default `0.0`).
  Rejected: it diverges from FDB parity and changes behaviour for every moonpool consumer, when the realism gap is magnetar's (plain connections + non-persistent test brokers), not moonpool's.
- **Targeted payload-only corruption** (a Pulsar-frame-aware moonpool).
  Rejected: moonpool's network sim is byte-level and protocol-agnostic by design; it cannot know which bytes are a CRC-bearing payload.
- **Reuse `OpOutcome::SessionLost` for the terminal outcome.**
  Rejected: `SessionLost` is load-bearing for the supervisor's transparent at-least-once replay — its `Send`-key carve-out in `reset()` keeps publishes pending for re-issue.
  Conflating the two would regress at-least-once publish parity.

## Decision

### §1 Plain connections fail fast, never hang

Add a terminal outcome `OpOutcome::Terminal { key, reason }`, distinct from the replay-oriented `OpOutcome::SessionLost`, and a sans-io entry point `Connection::fail_all_pending(reason)`.
On a **plain** driver's terminal exit — fatal decode, peer close, or I/O error — the engine resolves **every** pending op (`Request` keys, `Send` keys, and the consumer receive-waker slab) with `Terminal`, queues a `ConnectionEvent::Closed { reason }` so the event-stream waiters (`ProducerReady` / `SubscribeAcked`) unblock, and wakes all waiters.
Each engine maps `OpOutcome::Terminal` to `ClientError::PeerClosed`, so an in-flight `subscribe()` / `send()` / `receive()` returns promptly instead of stalling.

The event-stream waiters (`ProducerReady` / `SubscribeAcked` futures) park on `ConnectionEvent::Closed`, not on the op-outcome slab, so their `Closed` arm distinguishes the two producers of that event by `reason`: a `Closed { reason: Some(_) }` (the terminal drop `fail_all_pending` queues) surfaces `ClientError::PeerClosed` — the same terminal outcome the request / send / receive paths give — while a `Closed { reason: None }` (a user-requested graceful `close()`) keeps the pre-existing `ClientError::Closed`.
Before this amendment those waiters surfaced a generic `ClientError::Other(reason)` on either, which a caller could not tell apart from an unrelated engine error.

The supervised path is unchanged: a transient drop still routes through `reset()` for transparent at-least-once replay (ADR-0038).
`fail_all_pending` fires **only** on a genuinely-terminal exit — the plain spawn, a supervisor that has exhausted its attempts, or a user-requested close — never on the per-attempt reconnect.

### §2 Wire corruption is a recoverable transient drop

A corrupted, unparseable frame is treated exactly like a connection drop.
A **supervised** client recovers it through the existing reconnect → `reset()` → `rebuild_producers` / `rebuild_consumers` replay machinery (ADR-0038, `a02f401`).
No corruption-specific recovery path is added; corruption simply joins the set of faults the supervisor already survives.

### §3 `sim_chaos` workload tests run supervised over a persistent broker

Every bit-flip-exposed `sim_chaos` workload test that uses a plain client with delivery assertions is converted to a **supervised** client.
The in-sim broker persists its ledger + per-subscription cursor in an `Arc<Mutex<SharedBroker>>` keyed by **stable identity** — topic for the ledger + next-entry-id, subscription **name** for the cursor — that survives the per-session reset, resumes a re-subscribe from the acked cursor (`start_message_id = last_acked_message_id`, redelivering only un-acked messages), and dedups replayed publishes by `(topic, sequence_id)` (re-emitting the existing receipt, and recording the `SENDS_TRAIL` correctness fact only on the *first* acceptance so the monotonic-sequence-id invariant still holds).
The shared state is cleared per iteration in the broker workload's `Workload::setup` (one workload instance is reused across a seed sweep).
The consumer drain loops dedup received message ids by `(ledger_id, entry_id)` to absorb legitimate at-least-once redelivery.

Two test-harness mechanics fall out of the supervised conversion:

- the in-sim broker session loop races its socket read against a short injected-clock **dispatch tick** (`TimeProvider::sleep`, ADR-0011 — no host clock), so a redelivery that becomes available with no inbound traffic (a clog lifting after the producer is done, or a reconnect's replayed publish while a consumer sits in `receive()`) is still pushed; previously the broker only re-evaluated delivery when an inbound frame arrived;
- the workloads retry the setup-phase `subscribe` / `open_producer` across a transient drop. A bit-flip on the in-flight LOOKUP behind those calls surfaces as a transient `SessionLost` (the supervisor is reconnecting) that the engine does not transparently re-issue, so the workload re-issues the op against the freshly-handshaked session — mirroring the Java client's lookup-after-reset retry. This is setup-phase resilience only; it weakens no delivery / dedup assertion.

## Consequences

- A plain connection surfaces `PeerClosed` on any in-flight op at a terminal drop instead of hanging — a general robustness win, independent of chaos.
- The `sim_chaos` suite is no longer latently flaky to bit-flip: corruption is a recovered transient drop rather than a coin-flip hang.
  The two ADR-0038 anchor seeds pass again and **stay** as per-PR regression anchors — no registry edit (ADR-0047 §anchors).
- bit-flip chaos stays **on** (FDB parity); corruption resilience is still exercised, now via the supervised recovery path rather than by accident.
- ADR-0024 layers: §1 is a behavioral `magnetar-proto` change, so it ships with a proto unit test, both runtime integration tests (kept tokio↔moonpool 1:1), a `magnetar-differential` terminal-error equivalence test, and an e2e.
  §3 is sim-harness (test-support) code, validated by the converted workloads.

## References

- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime test + coverage policy.
- [ADR-0038](0038-split-connection-mutex.md) — split connection mutex; reconnect replay + lock-ordering.
- [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) / [ADR-0047](0047-known-failing-seed-registry.md) — e2e policy + known-failing seed registry / anchors.
- [ADR-0050](0050-swizzle-clog-workload.md) — the swizzle-clog workload whose sweep surfaced the failure.
- [ADR-0054](0054-logging-policy.md) — `reason` strings carry no secret material.
- moonpool `fix/no-progress-detector-busy-peer`: `maybe_corrupt_data`, `mark_connection_stable` (FDB `sim2.actor.cpp` `stableConnection`).
