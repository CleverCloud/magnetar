# ADR-0093 — PIP-460 on the upstream wire surface, negotiated per connection

- **Status**: Accepted (layout-epoch handling amended by [ADR-0095](0095-ignore-a-re-sent-scalable-layout-epoch.md); high-level data-plane scope and consumer-assignment epoch handling amended by [ADR-0098](0098-assignment-driven-m1-hardened-stream-consumer.md))
- **Date**: 2026-08-03
- **Decider**: Florentin Dubois
- **Tags**: pip-460, scalable-topics, segments, wire-format, vendored-proto, compatibility, experimental
- **Supersedes**: [ADR-0031](0031-pip-460-scalable-subscription-scope.md)

## Context

[ADR-0031](0031-pip-460-scalable-subscription-scope.md) locked magnetar's PIP-460 scope when upstream PIP-460 was `Draft` and no vendored proto carried it.
Its decision therefore described a **projected** wire surface: a `CommandScalableTopicLookup` / `…LookupResponse` pair, a separate `CommandSegmentDagWatch` subscribe handshake keyed by a `lookup_token`, `CommandSegmentDagUpdate` frames carrying explicit `SplitEvent` / `MergeEvent` deltas, six `BaseCommand.Type` discriminators at 80-85, and a `ProtocolVersion` bump to 22.
That surface was implemented as hand-encoded `prost` structs in `crates/magnetar-proto/src/pb/scalable_topics.rs`, behind an xtask carve-out (`PB_HAND_MAINTAINED_FILES`) that hid the file from the codegen drift check.

Upstream then landed PIP-460 and it does not look like that.

Two facts drove this ADR, both established on 2026-08-03:

1. **The blocker had already lifted, silently.** The vendored proto was refreshed to `7735851` on 2026-05-04, and that revision **already carried the real PIP-460 messages**. `crates/magnetar-proto/src/pb/pulsar.proto.rs` had been generating `SegmentInfoProto`, `SegmentBrokerAddress`, `ScalableTopicDag`, `CommandScalableTopicLookup`, `CommandScalableTopicUpdate` and `CommandScalableTopicClose` ever since. Nothing referenced them.
2. **The client spoke a protocol no broker implements.** Every consumer — `conn.rs`, `dag_watch.rs`, `types.rs`, both engines, the façade, `magnetar-fakes`, the CLI — used the hand-encoded module. Because `magnetar-fakes` implemented the _same_ fabricated protocol, all four ADR-0024 test layers plus the golden trace were green while the wire bytes could not have been parsed by any Pulsar broker, at any version.

`docs/follow-ups.md` §1 read this as "e2e is blocked on an upstream RC". The real state was worse and the opposite of blocked: the authoritative types were sitting in the tree, unused, behind a green test suite that could never have caught the divergence.

### How far the guess diverged

|                       | hand-encoded (was in use)                                  | upstream (Pulsar 5.0.0-M1)                                                                       |
| --------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `BaseCommand` types   | 80-85                                                      | 70-78                                                                                            |
| lookup request        | `topic=1, request_id=2, authoritative=3, original_*=4..6`  | `session_id=1, topic=2`                                                                          |
| response              | `CommandScalableTopicLookupResponse` + a `LookupType` enum | `CommandScalableTopicUpdate` — initial response **and** every pushed update                      |
| layout                | flat `repeated SegmentDescriptor`                          | `ScalableTopicDAG{epoch, segments, segment_brokers, controller_broker_url}`                      |
| segment               | one message carrying id, range, broker URL, state          | `SegmentInfoProto` (topology + lifecycle) **plus** a parallel `SegmentBrokerAddress` (placement) |
| segment state         | `Active` / `Splitting` / `Merging` / `Sealed`              | `ACTIVE` / `SEALED` only                                                                         |
| watch session         | separate subscribe handshake keyed by `lookup_token`       | none — the lookup **is** the subscribe, keyed by `session_id`                                    |
| DAG change            | `SplitEvent` / `MergeEvent` delta frames                   | whole-layout snapshot, ordered by a monotonic `epoch`                                            |
| capability gate       | `ProtocolVersion = 22`                                     | `FeatureFlags.supports_scalable_topics = 8`; `ProtocolVersion` still tops at `v21`               |
| consumer registration | absent                                                     | `CommandScalableTopicSubscribe` → `ScalableConsumerAssignment` → `…AssignmentUpdate`             |
| namespace watch       | absent                                                     | `CommandWatchScalableTopics` with a snapshot/diff `oneof`                                        |

Not one field number, message shape, or lifecycle assumption survived contact.

## Decision

### D1 — Vendor Pulsar 5.0.0-M1 and delete the hand-encoded module

Vendor `apache/pulsar` at `8dae0236c0a0d405ed7f8303081080520fe91551` (`v5.0.0-M1`, 2026-06-16), as a dedicated commit per [ADR-0026 §D4](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md), which sanctions milestone-pinned bumps.
`crates/magnetar-proto/src/pb/scalable_topics.rs` is **deleted**, not reconciled, and the `PB_HAND_MAINTAINED_FILES` carve-out in `xtask/src/main.rs` is removed with it, so `codegen --check` now covers every file under `pb/`.

The carve-out was load-bearing in a way worth recording: plain `cargo run -p xtask -- codegen` regenerates `pb/` wholesale and had always deleted `scalable_topics.rs`, while `codegen --check` skipped it — so the drift gate could not see the file _and_ an ordinary codegen run destroyed it. Removing both ends that asymmetry.

M1 is a milestone, not a GA release. Upstream may still change the surface before 5.0.0 final; the parity matrix keeps PIP-460 marked experimental, and a later revision is another §D4 bump.

### D2 — Client state machine follows the upstream shape

- **One session, no separate subscribe.** `Connection::open_scalable_topic_session(topic)` allocates a client-side `session_id`, emits `CommandScalableTopicLookup`, and installs an empty `DagWatchSession`. The broker replies with `CommandScalableTopicUpdate` and keeps pushing on the same session until `CommandScalableTopicClose`. The `lookup_token` handshake is gone.
- **Snapshots, not deltas.** `DagWatchSession::handle_update` replaces the layout wholesale, guarded by the monotonic `ScalableTopicDAG.epoch`, and derives what changed by diffing segment sets. Split and merge are read off the incoming layout's `parent_ids` / `child_ids` edges: a new segment naming one previously-held parent is a split of it (children grouped, so 1→N is one event); one naming several is a merge. Parents outside the observed window are ignored rather than guessed at.
- **Placement is optional.** `SegmentDescriptor::broker_url` becomes `Option<String>`, joined from the `segment_brokers` list by `segment_id`. A sealed segment the broker no longer serves carries no address entry.
- **`SegmentState` collapses to `Active` / `Sealed`.** `Splitting` and `Merging` have no wire representation; they were states in the projection and are topology transitions upstream.
- **Ordinary framing.** The commands are `BaseCommand` fields, so they encode through `encode_command` and decode through `decode_one`. The hand-built `ScalableBaseCommand` envelope and the frame-interception hook in `handle_bytes_decode_loop` are both deleted — a frame is a frame again.
- **Legacy layouts are carried through.** `SegmentInfoProto.legacy_topic_name` marks the broker's synthetic single-segment layout for a regular, unmigrated topic; `SegmentDescriptor::is_legacy()` surfaces it so the v4 topic name reaches the consumer.

### D3 — Compatibility is negotiated per connection, in both directions

A client compiled with `scalable-topics` sets `CommandConnect.feature_flags.supports_scalable_topics = true`.
`Connection::broker_supports_scalable_topics()` reads the broker's answer from `CommandConnected.feature_flags`, and **every** scalable-topic command is gated on it: `open_scalable_topic_session` returns `ScalableTopicError::BrokerUnsupported` and writes **nothing** to the outbound buffer when the peer did not advertise support.

This is what keeps a `scalable-topics` build usable against Pulsar 4.x, and it covers a 5.x broker started with `scalableTopicsEnabled=false` for free.
The fabricated `SUPPORTED_PROTOCOL_VERSION_SCALABLE_TOPICS = 22` is deleted: claiming a version above the v4 ceiling was itself a compatibility hazard, and upstream gates the feature on the flag while leaving `ProtocolVersion` at `v21`.

### D4 — The feature flag now gates client logic, not wire types

`magnetar-proto`'s `scalable-topics = []` Cargo feature previously gated the existence of the wire types, because they lived in a hand-maintained module.
The generated `pb/pulsar.proto.rs` is not feature-gated, so the PIP-460 messages are now always compiled.
The feature gates the **client surface** — session state, connection entries, engine and façade API — and nothing else. A default build's public API is unchanged.

### D5 — The rest of the V5 surface lands with it

Reading a layout is not the same as owning a share of it, so the client also implements the upstream commands that grant one:

- **Consumer registration.** `CommandScalableTopicSubscribe` → `…SubscribeResponse` carries the initial `ConsumerAssignment` (a `layout_epoch` plus the `segment://` topics this consumer owns), and `…AssignmentUpdate` pushes a fresh one after every rebalance, surfaced as an `AssignmentDelta` naming exactly what to attach to and detach from. An assignment whose `layout_epoch` does not advance is **rejected**, the same guard the layout session applies: the broker recomputes assignments per layout, so acting on an out-of-order push would attach the consumer to segments that no longer exist. `ScalableConsumerType` carries `Stream` and `Checkpoint` only — a `QueueConsumer` never registers, mirroring upstream.
- **Namespace watch.** `CommandWatchScalableTopics` delivers a snapshot then incremental diffs of the matching topic set. A diff applies `removed` **before** `added`, per upstream's own note; the reverse order drops a topic named in both lists.
- **Transaction-coordinator discovery** (PIP-473). `CommandWatchTcAssignments` is negotiated on its **own** flag, `supports_tc_metadata_discovery`, not on `supports_scalable_topics` — upstream advertises them independently, so a broker may serve scalable topics without it. PIP-473's `scalable` routing bit on the PIP-31 transaction commands is sent absent, selecting the legacy coordinator this client speaks today.

## Consequences

**Breaking.** The `magnetar::proto` surface changes shape: `SegmentDescriptor` gains fields and its `broker_url` becomes `Option`, `SegmentState` loses two variants, `DagDelta` gains `epoch`, `SplitEvent` / `MergeEvent` lose their `*_at_entry` fields, `DagError` swaps `UnknownSegment` for `Broker` / `Empty`, and every `watch_session_id` becomes `session_id`. Engine and façade surfaces follow. Carried with a `BREAKING CHANGE:` footer and a `CHANGELOG.md` entry per the API-stability stance in `docs/follow-ups.md`.

**The four test layers were rewritten, not adjusted.** Every scripted broker — `magnetar-fakes`, both engine tests, the differential transcript — now emits real upstream frames, so a green run is evidence about the actual protocol for the first time. The golden trace was regenerated; its byte stream necessarily changed.

**e2e is unblocked.** `apachepulsar/pulsar:5.0.0-M1` is published and defaults `scalableTopicsEnabled` to `true`, and M1's admin API carries `createScalableTopic(topic, numInitialSegments)` and `splitSegment(topic, segmentId)` — a real trigger for the drop-on-change test. `docs/follow-ups.md` §1 closes.

**Still out of scope**, unchanged from ADR-0031: transparent segment failover, in-place repartition under load, and controller-election awareness. Drop-on-change remains the contract.

**Still not implemented** and left as follow-up work: `QueueConsumer` and `CheckpointConsumer` surfaces on the façade (the wire type exists, the consumer does not), and the per-segment consumer fan-out that would let `StreamConsumer` actually receive messages from its assigned segments rather than only observe the assignment.

**`MessageIdData` was not extended.** ADR-0031 projected an `Option<SegmentId>` on `MessageId`; M1's `MessageIdData` carries no segment field. Segment identity travels in the `segment://` topic name instead, so no `MessageId` change is needed and none was made.

## Status

Accepted (2026-08-03).

## References

- [ADR-0031](0031-pip-460-scalable-subscription-scope.md) — superseded by this ADR. Its scope decisions (StreamConsumer-only, drop-on-change, experimental tag) survive; its wire-surface description does not.
- [ADR-0026 §D4](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) — milestone-pinned vendor bumps; the mechanism used here.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — four-layer test policy binding on this change.
- [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) — e2e carries no `#[ignore]` and no separate feature.
- [ADR-0009](0009-pulsar-4-minimum.md) — Pulsar 4.0+ minimum, which D3's negotiation preserves.
- PIP-460 (Scalable Topics) — <https://github.com/apache/pulsar/blob/master/pip/pip-460.md>
- PIP-473 (scalable transaction coordinator) — the `scalable` bit on the PIP-31 transaction commands.
- PIP-483 (scalable-topic auto split/merge) — the broker-side policy that drives layout changes.
- Upstream release `v5.0.0-M1` — <https://github.com/apache/pulsar/releases/tag/v5.0.0-M1>
