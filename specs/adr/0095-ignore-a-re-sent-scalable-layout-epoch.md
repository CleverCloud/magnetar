# ADR-0095 — Ignore a re-sent scalable layout epoch instead of ending the watch

- **Status**: Accepted (the consumer-assignment comparison is superseded by [ADR-0102](0102-assignment-driven-m1-hardened-stream-consumer.md); the duplicate-layout decision remains binding)
- **Date**: 2026-08-04
- **Decider**: Florentin Dubois
- **Tags**: pip-460, scalable-topics, epoch, watch-session, wire-format, experimental
- **Amends**: [ADR-0093](0093-pip-460-upstream-wire-surface.md) § D2 (epoch handling only; every other decision in it stands)

## Context

[ADR-0093](0093-pip-460-upstream-wire-surface.md) § D2 decided that `DagWatchSession::handle_update` replaces the layout wholesale "guarded by the monotonic `ScalableTopicDAG.epoch`".
That was implemented as a **strict** guard: an update whose `epoch` did not exceed the applied one returned `DagError::NonMonotonic`.
`Connection` closes a scalable-topic session on **any** update error, so the guard did not merely discard the frame — it ended the watch.

Against a real `apachepulsar/pulsar:5.0.0-M1` broker that is the first thing that happens.

The broker answers `CommandScalableTopicLookup` with the current layout, and then pushes **that same layout, at that same epoch**, on the watch the lookup just opened.
The lookup response and the first pushed update are the same snapshot.
So the client resolved at epoch 0, immediately received epoch 0 again, called it a protocol violation, and tore its own watch down — after which it saw nothing at all.

Measured on CI, from the diagnostics in `e2e_scalable_topic_drops_on_broker_split`:

```
epoch before: 0, last pushed epoch: 0
events observed in 60s (1): DagWatchClosed { session_id: 1,
  reason: Some("scalable-topic update rejected: non-monotonic layout epoch: got 0 expected > 0") }
broker layout on re-lookup: epoch=2, segments=[1(parents=[], children=[], state=Active),
                                               2(parents=[], children=[], state=Active)]
```

The broker had split and advanced to epoch 2. The client had stopped listening at epoch 0.

The premise was wrong, not the observation. "Monotonic" constrains the **order** the broker publishes layouts in; it never promised a strictly increasing value per frame, and nothing forbids re-sending a snapshot the peer already has. A duplicate is idempotent: same epoch, same layout, nothing to apply.

**Why no test caught it.** Every scripted transcript across all four ADR-0024 layers advanced the epoch on every frame, so not one of them ever sent the duplicate a real broker sends first. `session_guards_and_accessors_parity` went further and asserted the teardown as _intended behaviour_. This is the same failure this branch already fixed once — a test double that only replays the shapes we imagined certifies nothing (see ADR-0093 § Context) — and it is why the e2e against a real broker is load-bearing rather than decorative.

## Decision

A layout `epoch` that does not advance is **ignored**, never fatal.

- `DagWatchSession::handle_update` returns `Ok(None)` when `epoch <= applied`. The layout, the resolved topic name and the controller URL are untouched, no `DagDelta` is produced, and the caller emits no event.
- The session **stays open**. Only `DagError::SessionMismatch`, a broker-side error, or a bodyless update ends one.
- `DagError::NonMonotonic` is removed; it has no remaining meaning.
- `handle_update` therefore returns `Result<Option<DagDelta>, DagError>`.

This covers a stale frame as well as a duplicate. Both mean "a layout this session already accounts for", and neither is worth a teardown; the strictly-older case additionally cannot arrive over an ordered stream on one connection.

### The consumer-assignment guard is deliberately not changed

ADR-0093 § D2 describes the consumer-assignment check as "the same guard the layout session applies". After this ADR the two differ, on purpose:

|                                        | non-advancing epoch              | blast radius                                                        |
| -------------------------------------- | -------------------------------- | ------------------------------------------------------------------- |
| layout (`DagWatchSession`)             | ignored, session survives        | a teardown here blinds the client to every later layout             |
| assignment (`ScalableConsumerSession`) | rejected, surfaced to the caller | the caller learns its assignment did not move; nothing is torn down |

A layout is a snapshot the session _holds_; re-receiving it is a no-op. An assignment is an instruction the consumer _acts on_, and acting on an out-of-order one attaches it to segments that may no longer exist. The assignment rejection also does not close a session, so it does not carry the failure mode this ADR exists to remove. If a real broker is later observed re-sending an identical assignment, that is its own measurement and its own decision — it has not been observed, so nothing is changed on speculation.

## Consequences

**Easier.** A `scalable-topics` client survives contact with the broker it was written for. Drop-on-change works, which is the whole point of the surface: `e2e_scalable_topic_drops_on_broker_split` passes against a real 5.0.0-M1 broker, having failed on every prior run.

**Harder.** Nothing measurable. The removed guard protected against a case that cannot occur on an ordered per-connection stream, and the session already diffs each snapshot rather than trusting it to be new.

**Cost.** One public signature and one removed error variant, both behind the default-off `scalable-topics` feature, so a default build's API is unchanged. Recorded as a `BREAKING CHANGE` with a `CHANGELOG.md` entry.

**Test debt repaid.** All four layers now carry the duplicate explicitly — proto unit, both engines (1:1 parity preserved at 362/362), and a differential transcript asserting both engines ignore it identically and still observe the split that follows. The regression was seen red before green: restoring the guard fails the mirrored engine tests on "a duplicate epoch must not close the session".

**Incompatible with.** Any future reading of `ScalableTopicDAG.epoch` as strictly increasing per frame. It is an ordering, not a counter of frames.

## References

- `crates/magnetar-proto/src/dag_watch.rs` — `handle_update`, and the note on `DagError` recording why there is no `NonMonotonic`.
- `crates/magnetar-proto/src/conn.rs` — the `Ok(None)` arm that keeps the session.
- `crates/magnetar/tests/e2e_scalable_topic.rs` — `e2e_scalable_topic_drops_on_broker_split`, the only test that could have caught this.
- `crates/magnetar-differential/tests/scalable_topic_equivalence.rs` — `duplicate_layout_epoch_event_stream_parity`, and `session_guards_and_accessors_parity` corrected.
- [ADR-0093](0093-pip-460-upstream-wire-surface.md) — the surface this amends.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the four-layer policy the duplicate now appears in.
- [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) — why the e2e runs on every push, which is what surfaced this.
