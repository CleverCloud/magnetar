# Audit: quasar plan + review

> Final-gate pass over `/home/florentin/.claude/plans/ask-quasar-plan.md` (the plan) and `/home/florentin/.claude/plans/ask-quasar-review.md` (the reviewer's report), against the dossier `/home/florentin/.claude/plans/ask-quasar-research.md`, the live Pulsar source at `/home/florentin/Sources/github.com/apache/pulsar`, the empty repo at `/home/florentin/Sources/github.com/me/quasar`, and `~/.claude/CLAUDE.md`.

---

## A. Final verdict

**APPROVED with required reviewer fixes folded in.** The plan + reviewer's report together describe a coherent, citation-backed, scope-appropriate v0.1.0 driver. The architecture (sans-io `quasar-pulsar-proto` + tokio/moonpool engines), the PIP scope (22 wire-affecting PIPs for v0.1.0; 8 deferred), the seven-milestone schedule, and the 20+ approval gates all hold up. Reviewer's 10 must-fixes (C-1–C-10) are real but small; six fold directly into the plan as in-place edits, two are user-decisions that must surface in the question list, two are deferrable to the implementer with explicit notes. The plan is **not** ready to start coding without a single editing pass (folding C-1–C-4, C-6, C-7, C-10 into the plan text) and a single user-decision round on the consolidated question list in §D. Once those two steps complete, M0 can begin.

---

## B. Standards & RFC-sensitive gaps

| Concern | Verdict | Justification |
|---|---|---|
| Wire frame format (simple + send/message + 0x0e01 envelope) | **PASS** | Plan §3 steps 3 mirrors `Commands.java:1866-1934`; magic 0x0e01 + CRC32C + metadata-size + metadata + payload all specified. Roundtrip tests called out in §3 tests. |
| 0x0e02 broker-entry-metadata detection | **PASS** | Plan §3 step 3 + §4 step 2 `handle_message` peel order: 0x0e02 → 0x0e01. Tied to FeatureFlag `supports_broker_entry_metadata` (PIP-90) in §4 step 4 (consumer state). |
| CRC32C verify | **PARTIAL** | Plan uses the `crc32c` crate (Castagnoli, hardware-accelerated). **Missing**: explicit statement that the 4-byte CRC field is **big-endian** on the wire (Netty default) and that the polynomial matches Java's `io.netty.handler.codec.compression.snappy.Crc32C` (Castagnoli reflected, same as Linux kernel / SSE4.2). The dossier §2 mentions polynomial; the plan §3 step 3 should explicitly call out byte order so the implementer doesn't accidentally write little-endian. **Fix**: add one line in plan §3 step 3 wire-layout note: "CRC32C value is written big-endian as a `u32` field; matches Netty default and Java client." |
| ProtocolVersion negotiation (claim V21, accept lower) | **PASS** | Plan §4 step 2 fields `protocol_version_claimed: i32 = ProtocolVersion::V21 as i32`. **Implicit**: on CONNECTED, server may downgrade — `handle_connected` is supposed to capture `server_protocol_version` (the field exists at `CommandConnected` per `PulsarApi.proto`). Plan should state explicitly that negotiated version is `min(client, server)` and that handlers gate PIP-specific behavior on it. **Improvement** (not a fail). |
| FeatureFlags negotiation (8 flags per dossier §2) | **PASS** | All 8 flags listed in dossier §2 cited at proto:311-320; plan §4 step 2 captures `feature_flags_negotiated: pb::FeatureFlags`. Plan §4 step 7 (`LookupStateMachine`) gates PIP-344 on `supports_get_partitioned_metadata_without_auto_creation`. Plan §4 step 4 (`Consumer`) gates PIP-90 peeling on `broker_entry_metadata_enabled` derived from `supports_broker_entry_metadata`. Other flags (auth refresh, partial producer, repl dedup, topic watcher reconcile, scalable topics) need similar gates — **fold** into M2 implementer notes. |
| AUTH_CHALLENGE / AUTH_RESPONSE (PIP-30, PIP-292) | **PASS with risk note** | Plan §4 step 2 + §8 wire it. Reviewer's §H (and audit §E below) calls out the in-flight refresh edge case. Test `auth_refresh_during_chunked_send.rs` is recommended but missing from §10 test list. **Fix**: add it. |
| Chunking (PIP-37 + PIP-107 + PIP-131) | **PASS** | Plan §4 step 3 producer-side: assign `MessageMetadata.{uuid, chunk_id, num_chunks_from_msg, total_chunk_msg_size}`. Plan §4 step 4 consumer-side: `chunks_in_flight` reassembly with `max_chunks_in_flight = 100` cap. PIP-107 `first_chunk_message_id` mentioned in §10 test 6. PIP-131 (oversized vs topic-max) called out in PIP scope §0. |
| Key_shared full surface (PIP-34 + PIP-119 + PIP-282 + PIP-379) | **PARTIAL** | Plan §4 step 4 mentions `key_shared mode` in `SubscriptionSpec`. PIP-119 (consistent hashing default) is a *broker* default change — client just sends `KeySharedMode::AutoSplit` and accepts. PIP-282 initial position is on `CommandSubscribe.initial_position` — plan does not explicitly say the consumer's `SubscriptionSpec` exposes a per-subscription override. PIP-379 (draining hashes) is explicitly broker-driven (the consumer is *observer*); the plan must state that the consumer state machine does **not** assume key→consumer affinity is stable, and must surface re-routing as a normal event, not an error. **Fix**: add one paragraph in §4 step 4 describing PIP-379 semantics ("broker dictates dispatch; client treats received messages as authoritative regardless of prior key affinity"). |
| Batch-index ACK (PIP-54 + PIP-391 + ACK_RESPONSE) | **PARTIAL** | Plan §4 step 5 wires `ack_tracker.rs` with `ack_set` bitset. Reviewer's C-5 flags ambiguity about where the bitset lives — confirmed: `ack_set` appears on `MessageIdData` (proto:64) **and** on `CommandAck` (proto:569). The `CommandAck.ack_set` (proto:569, repeated int64) is PIP-54's original cumulative-ack bitset; `MessageIdData.ack_set` (proto:64, repeated int64) is for batched message-id carrying. PIP-391 added finer per-batch state on top. Plan §4 step 5 must distinguish the two — **fold C-5**. ACK_RESPONSE handler at §4 step 2 `handle_ack_response` exists. |
| DLQ + retry-topic (PIP-22 + PIP-58 + PIP-124 + PIP-409) | **PASS** | Plan §4 step 4 consumer-side `dead_letter_policy`, `retry_letter_policy`, `redelivery_for_dlq`. Correctly notes broker is mostly unaware (PIP-22/124). PIP-409 (producer config for retry/DLQ producer) is a config surface only — plan §8/§7 covers via builder. |
| TopicListWatcher (PIP-145) — WATCH_TOPIC_LIST/SUCCESS/UPDATE/CLOSE | **PASS** | Plan §4 step 9 `TopicListWatcher`; §4 step 2 `handle_watch_topic_list_*`. Test in §10. |
| TOPIC_MIGRATED (PIP-188) | **PASS** | Plan §4 step 2 `handle_topic_migrated` emits `ConnectionEvent::Reconnect { target }`. |
| getLastMessageIds for partitioned readers (PIP-296) | **PASS** | Plan §4 step 2 `handle_get_last_message_id_response`. Reader API surface in M3 will expose the partitioned variant. |
| Force unsubscribe (PIP-313) | **PARTIAL** | Plan v0.1.0 PIP scope §0 lists PIP-313. **Missing**: explicit mention in §4 (Connection or Consumer state machine) that `CommandUnsubscribe.force` is wired. One-line fix. |
| Force-no-auto-create on getPartitionsForTopic (PIP-344) | **PASS** | Plan §4 step 7 explicit: "emit `CommandPartitionedTopicMetadata.metadata_auto_creation_enabled = false` when the FeatureFlag is negotiated." |
| Apache-2.0 license headers on every `.rs` | **PASS** | Plan §12 specifies `// SPDX-License-Identifier: Apache-2.0` on every `.rs`; `xtask license-check` enforces. |
| CLAUDE.md conformance — no-claude-attribution, conventional+signed commits, validation chain, worktree-first, no-edits-on-`main`-post-M0 | **PASS** | Plan §2 step 14 + §15 item 20 + final checklist all hit these. M0 initial commit exemption is correct (empty repo, hook short-circuits — verified). |

**Net B-section result**: PASS on 13/17, PARTIAL on 4/17. All four partials are fold-into-plan edits with one-to-two-line scope.

---

## C. Documentation follow-up

| Doc | In plan? | Notes |
|---|---|---|
| `README.md` (quickstart + status banner + supported-PIPs link) | **in plan** | Plan §11. |
| `LICENSE` | **in plan** | Plan §2 step 2. |
| `NOTICE` | **in plan** | Plan §2 step 3. |
| `GUIDELINES.md` (protocol invariants, code style, validation chain, PIP support matrix) | **in plan** | Plan §11 expands the content. |
| `AGENTS.md` (default branch, wt usage, no-Claude-trailer rule, validation chain, GUIDELINES.md link) | **in plan** | Plan §2 step 14. |
| `CHANGELOG.md` (Keep-a-Changelog) | **in plan** | Plan §11. |
| `CONTRIBUTING.md` (branch, commit, validation, **`protoc >= 3.19` requirement** per reviewer C-4) | **partially in plan** | Listed in §11 but content missing for `protoc` requirement. **Fold**: add `protoc >= 3.19` instruction. |
| `docs/architecture.md` (sans-io diagram, quinn-proto-style API map) | **in plan** | Plan §4 docs-to-update. Reviewer's nice-to-have ("For users coming from quinn-proto" section) is worth folding. |
| `docs/protocol.md` (wire-format reference) | **in plan** | Plan §3 docs-to-update. |
| `docs/quickstart.md` (first producer/consumer) | **in plan** | Plan §5 docs-to-update. |
| `docs/schema-support.md` (supported/not-yet matrix) | **in plan** | Plan §7 step 4. |
| `docs/simulation-testing.md` (moonpool-sim usage) | **in plan** | Plan §6 docs-to-update. |
| `docs/migration-from-pulsar-rs.md` | **in plan** | Plan §11 + final checklist. **Important** because Florentin is the pulsar-rs maintainer — this is the pulsar-rs user's path to quasar. |
| Rustdoc on every `pub` item in `quasar-pulsar-proto`, with `#![deny(missing_docs)]` | **in plan** | Plan §11 + reviewer's improvement note (deny at M1 not M2 — fold). |
| "Supported PIPs" matrix in `README.md` (top-level) | **partially in plan** | `GUIDELINES.md` has it (§11). The reviewer asked for it in `README.md`. **Fold**: link the matrix from `README.md` to `GUIDELINES.md`. |
| `docs/auth.md` (token, TLS, AUTH_CHALLENGE flow) | **missing** | Auth has its own milestone but no dedicated doc. **Fold**: add `docs/auth.md` to M6 deliverables. |
| `xtask check-vendor` doc note in `CONTRIBUTING.md` (proto drift detection) | **missing** | Reviewer's nice-to-have. **Fold** (or accept as v0.2.0 work; explicit either way). |

---

## D. Explicit permission gates the user MUST decide

Consolidated from dossier §11 (15 Qs), reviewer's §F (22 items), and plan §15 (20 items). Collapsed duplicates; defaults marked. ≤12 items.

**Q1.** **Published crate name.** *Default: `quasar-pulsar` (drives all 6 crate names: `quasar-pulsar`, `quasar-pulsar-proto`, `quasar-pulsar-runtime-tokio`, `quasar-pulsar-runtime-moonpool`, `quasar-pulsar-admin`, `quasar-pulsar-fakes`). Impact if user defers: M0 cannot start — no `Cargo.toml` can be written.*

**Q2.** **License.** *Default: Apache-2.0 only. Impact if user defers: M0 cannot start — `LICENSE`, `NOTICE`, every `.rs` SPDX header, and `cargo deny` policy all need it.*

**Q3.** **Repo hosting + `gh repo create` permission.** *Default: `github.com/me/quasar` (Florentin's personal). Alternatives: `github.com/CleverCloud/quasar` (org) or a fresh Clever Cloud OSS org. Impact if user defers: M0 commit cannot be pushed; `Cargo.toml.workspace.package.repository` is wrong.*

**Q4.** **Engine set + public-default + moonpool risk acceptance.** *Default: tokio + moonpool, tokio is public default; moonpool is opt-in for deterministic sim (accepting its "hobby-grade" self-label). Impact if user defers: §1 crate topology and the `quasar-pulsar` façade's feature flags are undecided.*

**Q5.** **Crate split granularity.** *Default: 6-crate split as proposed (proto / façade / runtime-tokio / runtime-moonpool / admin / fakes + xtask). Alternative: single-crate-with-features. Impact if user defers: M0 stub-crate creation is blocked.*

**Q6.** **Minimum supported broker version.** *Default: Pulsar 3.0 LTS (matches Apache's own `PulsarContainer.java:66` pin). Impact if user defers: M2 PIP scope, `ProtocolVersion::V21` claim, and FeatureFlags coverage are unresolved.*

**Q7.** **v0.1.0 scope: schemas + transactions + encryption + auth.** *Default: schemas = bytes + String + Json + raw Avro/Protobuf; transactions = deferred to v0.2.0; encryption (PIP-4) = deferred to v0.2.0; auth = token + TLS + AUTH_CHALLENGE refresh. Impact if user defers: M2 inclusion of TC client + MessageMetadata encryption fields is undecided; M5/M6 surface is undecided.*

**Q8.** **`Cargo.lock` policy + `protoc >= 3.19` as contributor requirement (per reviewer C-4).** *Default: commit `Cargo.lock`; require `protoc >= 3.19` locally for contributors who run `xtask codegen`. Impact if user defers: M0 `.gitignore` and `CONTRIBUTING.md` are blocked.*

**Q9.** **moonpool TLS strategy.** *Default (reviewer's preferred): **option (d) — local rustls adapter over moonpool's byte pipe** (deterministic TLS handshake in sim); fall back to (c) TLS-less moonpool if (d) proves too costly. The plan's current default is (c). Impact if user defers: M4 moonpool sim cannot exercise `pulsar+ssl://` paths.*

**Q10.** **Admin REST + CLI + e2e provisioning.** *Default: admin scaffolded but unpublished for v0.1.0; CLI deferred to v0.2.0; e2e via `testcontainers-rs` + `docker-compose.yml` fallback. Impact if user defers: §9 (admin) and §10 (test strategy) are blocked.*

**Q11.** **Coexistence with pulsar-rs.** *Default: ship as separate project; document the migration path in `docs/migration-from-pulsar-rs.md`; revisit upstream/replacement after v0.1.0. Impact if user defers: the v0.1.0 announcement narrative + repo README positioning are blocked.*

**Q12.** **Allow-list expansion + per-step approval gates (per reviewer C-8).** *Default: expand allow-list with `prost-build`, `prost-types`, `tokio-util`, `futures` / `futures-util`, `pin-project-lite`, `tracing-subscriber`, `anyhow`, `clap`. Approval-gated per-action: `gh repo create`, `git push`, `cargo publish`, any dep outside the expanded allow-list, every `wt merge -y`. Impact if user defers: M2/M3/M4 implementer hits per-commit approval friction.*

---

## E. Unresolved questions still needing user direction (reviewer's §H)

These are reviewer-flagged biggest doubts that must not slip into the implementer's hands without user input:

1. **AUTH_CHALLENGE during chunked send / mid-flight token refresh.** Reviewer's §H is right that the plan understates this. The Java client handles it across `ClientCnx.java:464` + `ProducerImpl.java:1570` + `HandlerState.ProducerFenced`. **Question for user**: is the in-flight refresh edge case a v0.1.0 blocker (must have a dedicated test + correct dedup-ledger behavior) or a v0.2.0 follow-up (accept that the v0.1.0 driver may have a window where chunked sends fail to dedup correctly across a token refresh)? *Audit recommendation*: **v0.1.0 blocker.** Kubernetes service-account tokens rotate every ~60 minutes; chunked messages over a slow link can exceed that. Quasar must handle it from day 1 or it will look broken in real deployments. Add `auth_refresh_during_chunked_send.rs` to §10 test list. *Default if user defers*: treat as v0.1.0 blocker per audit recommendation.

2. **key_shared draining-hashes (PIP-379) — broker-driven dispatch.** Plan §4 step 4 + §10 test 10 cover the surface but don't explicitly state that the *client* has no fairness layer; the broker dictates which consumer gets each key. **Question for user**: is "broker dictates, client observes" acceptable, or do you want a client-side fairness wrapper (queue per active key, configurable max-keys-per-consumer)? *Audit recommendation*: **broker-dictates-only for v0.1.0.** PIP-379 is explicit that draining is a server feature; a client-side fairness layer would re-implement the broker's logic and likely diverge. Document the semantics in `GUIDELINES.md` and add an integration test that asserts client tolerance, not client policy. *Default if user defers*: broker-dictates-only.

3. **MessageCrypto deferred — but consumer must still detect encrypted messages cleanly (PIP-4 detection without decryption).** A consumer that gets an encrypted message in v0.1.0 must not panic; it must surface `ConsumerError::EncryptedMessageNotSupported` and let the application choose `cryptoFailureAction = CONSUME | DISCARD | FAIL` (Java's enum). Verified at `ConsumerImpl.java:85, :194, :2030`. **Question for user**: must v0.1.0 consumer at minimum *detect* encryption and route per `cryptoFailureAction`, or is "panic-free pass-through of `encryption_keys` field" enough? *Audit recommendation*: **detect + surface per `cryptoFailureAction`.** Trivial cost (a field-presence check + an enum branch), large user-experience win. *Default if user defers*: detect + surface.

4. **ZeroQueueConsumer (`receiver_queue_size = 0`) — Java has a dedicated subclass.** Plan §4 step 4 does not call this out as a special path. Java's `ZeroQueueConsumerImpl.java` short-circuits the receiver-queue logic to satisfy synchronous `receive()` semantics. **Question for user**: is `receiver_queue_size = 0` supported in v0.1.0 or rejected with a config error? *Audit recommendation*: **support in v0.1.0** because pulsar-rs supports it and migrating users will hit this. Cost: a small branch in the consumer state machine. *Default if user defers*: support.

5. **Unsolicited CLOSE_PRODUCER (broker fences us) — `HandlerState::ProducerFenced` exists in Java.** Verified at `ClientCnx.java:900` `handleCloseProducer` + `HandlerState.java:47` (`ProducerFenced` state) + `ProducerImpl.java:2115-2116` (state transition on `ProducerFencedException`). Plan §4 step 2 lists `handle_close_producer` but does not enumerate the unsolicited path or its interaction with `pending_sends`. **Question for user**: must v0.1.0 producer correctly drain `pending_sends` with `ProducerFencedException` on unsolicited close, or is "fail all pending with generic disconnect error" acceptable? *Audit recommendation*: **drain with `ProducerFencedException`.** Mirrors Java exactly; small cost. *Default if user defers*: drain with `ProducerFencedException`.

---

## F. Reviewer's required changes — disposition

| Reviewer item | Disposition | Notes |
|---|---|---|
| **C-1** `apache-avro = "0.21"` (not 0.17) | **Fold into plan now** | Plan §1 workspace deps; one-character edit. |
| **C-2** `testcontainers = "0.27"` (not 0.20) | **Fold into plan now** | Plan §1 workspace deps. |
| **C-3** `resolver = "3"` vs `rust-version = "1.85"` clarification + `incompatible-rust-versions = "fallback"` behavior note | **Fold into plan now** | Plan §1 + §2 step 5 + new `GUIDELINES.md` paragraph. |
| **C-4** `prost-build` strategy ambiguity (require `protoc >= 3.19` locally; `xtask codegen --check` in CI) | **Surface as user question (Q8 above) + fold accepted answer into plan** | Reviewer's option (a) is the right answer; surface to user, then fold. |
| **C-5** PIP-54/391 — clarify `MessageIdData.ack_set` vs `CommandAck.ack_set` | **Fold into plan now** | Confirmed two distinct fields: `MessageIdData.ack_set` at proto:64 + `CommandAck.ack_set` at proto:569. Plan §4 step 5 must say which is the batch-bitset path. |
| **C-6** Add `cargo audit` to CI **or** explicitly say `cargo deny check advisories` is the v0.1.0 substitute | **Fold into plan now** | Audit recommendation: keep `cargo deny check advisories` for v0.1.0; revisit `cargo audit` for v0.2.0. Make explicit. |
| **C-7** `RUSTDOCFLAGS="-D warnings" cargo doc` (not `cargo doc -D warnings`) | **Fold into plan now** | Plan §12 CI matrix; one-line edit. |
| **C-8** Allow-list expansion (`prost-build`, `prost-types`, `tokio-util`, `futures`, `pin-project-lite`, `tracing-subscriber`, `anyhow`, `clap`) | **Surface as user question (Q12 above) + fold accepted answer into plan** | Without it, the implementer is gated on every commit. |
| **C-9** moonpool TLS — add option (d) "local rustls adapter over moonpool's byte pipe" and re-evaluate default | **Surface as user question (Q9 above)** | Reviewer is correct: (d) is technically cleaner. Defer the decision to the user; default to (d) per audit. |
| **C-10** Worktree-per-stream contract for parallel M2a–M2d / M3 / M4 | **Fold into plan now** | Plan §13; one-sentence addition. |

**Net F-section result**: 7 fold-now, 3 surface-as-question-then-fold. Zero rejections.

---

## G. Risks the audit found that review missed

1. **CRC32C byte order on the wire is not specified by the Pulsar binary-protocol docs.** Verified via WebFetch on <https://pulsar.apache.org/docs/3.0.x/developing-binary-protocol/> — the spec says only "A CRC32-C checksum of everything that comes after it" without committing to endianness. Java/Netty defaults to big-endian for fixed-width fields, but the `crc32c` Rust crate returns a `u32` host-order value that the caller serializes. **A Rust implementation that writes little-endian will silently pass its own roundtrip tests and fail against a real Pulsar broker.** *Fix*: add a test `frame::encode_send_then_compare_against_java_byte_vector` (capture a Java-encoded frame for a fixed message and assert byte equality), and explicitly write the CRC field as `u32::to_be_bytes`.

2. **Checked-in `prost`-generated code is not tested across `prost` versions.** Plan §3 step 2 commits `src/pb/*.rs` to git. If `prost` goes from 0.13 → 0.14, the regenerated code shape (oneOf wrappers, enum repr, `optional` field codegen) may shift. The `xtask codegen --check` job will catch the diff in CI, but the *response policy* is undefined (block merge? open PR? warn?). *Fix*: add a `GUIDELINES.md` paragraph: "prost-version bumps are workspace-wide and require `cargo xtask codegen` + a single commit containing the new generated code; never bump prost without re-running codegen."

3. **`apache-pulsar:3.0.x` container on aarch64 — confirmed multi-arch.** Verified via Docker Hub: `linux/amd64` and `linux/arm64` ship for 3.0.11 through 3.0.17. No risk; document in `docs/contributing.md` so contributors on M-series Macs / aarch64 servers know they're supported.

4. **Lookup AND data connections both go through `tokio-rustls`.** Plan §5 step 2 covers data-connection TLS but does not explicitly say the **lookup** connection (`BinaryProtoLookupService.java:56` — `pulsar+ssl://` URIs) also uses TLS. The lookup path opens a fresh TCP+TLS connection per redirect. *Fix*: add a sentence to M3 step 2 making this explicit.

5. **Unsolicited CLOSE_PRODUCER (broker fences us)** — see audit §E item 5. Reviewer covered the auth refresh but not this.

6. **MessageCrypto detection-without-decryption for v0.1.0 consumer** — see audit §E item 3.

7. **ZeroQueueConsumer (`receiver_queue_size = 0`) special path** — see audit §E item 4.

8. **`xtask codegen` reproducibility.** The plan does not require `prost-build` invocations be **deterministic** (some prost versions emit timestamp comments). *Fix*: add `prost-build::Config::format(true)` and verify the output is byte-stable across two consecutive `xtask codegen` runs.

9. **`rustls` v0.23 deprecated `ServerName::try_from(&str)` for `&[u8]`.** Reviewer's §H notes this briefly. *Fix*: spell out the connect path in M3 step 2: `let server_name = ServerName::try_from(host.to_owned())?;` (the `try_from` for `String` is the v0.23 path).

10. **moonpool `async-trait` bleed into `quasar-pulsar` public API.** Plan §6 step 5 mentions the risk but the mitigation ("BoxFuture-typed wrappers inside `quasar-pulsar-runtime-moonpool` only") needs a CI assertion. *Fix*: add a `cargo tree -p quasar-pulsar | grep -v async-trait` check (assert async-trait not transitively pulled into the façade).

---

## H. Codex cross-check readiness

The plan **is ready** for the optional `--with-codex` step. The 5 most valuable questions to ask Codex:

1. **Sans-io decomposition shape**: is the `quinn-proto`-shape (`poll_transmit` / `handle_event` / `poll_event` / `poll_timeout` / `handle_timeout`) the right fit for a **message-broker** protocol (multiplexed producer/consumer channels over a single TCP connection), or should we look at `h2`'s `Connection` + per-stream `SendStream`/`RecvStream` decomposition instead, since Pulsar's producer/consumer ids are stream-like?

2. **moonpool engine adapter point**: is depending only on `moonpool-core::NetworkProvider` + `moonpool-core::TimeProvider` (skipping `moonpool-transport`'s NetTransport because its CRC32C-length-prefixed wire format conflicts with Pulsar's) the cleanest adapter, or does Codex see a sharper boundary?

3. **Chunking + batching + dedup + encryption interaction order** in the Java producer: the canonical send-path is (1) assign sequence-id (dedup), (2) optionally chunk if size > maxMessageSize, (3) optionally batch within a chunk (or chunk within a batch?), (4) optionally encrypt the payload (PIP-4). Verify against `ProducerImpl.java:419` (`sendAsync`) and `ProducerImpl.java:1570` (`ChunkedMessageCtx`) which order the JCl actually applies — the audit wants this nailed before M2 starts so the Rust state machine matches byte-for-byte.

4. **Schema-registry version negotiation**: does `GET_OR_CREATE_SCHEMA` round-trip work when client and broker disagree on `SchemaInfo` bytes ordering (Avro JSON whitespace, Protobuf descriptor canonicalisation)? Codex should compare the dossier's planned flow against `pulsar-broker/src/main/java/org/apache/pulsar/broker/service/schema/SchemaRegistry.java` to flag any version-equality footgun.

5. **`cargo-mutants` in v0.1.0 or v0.2.0?** Audit recommendation is v0.2.0 (after M2 stabilises) but the sans-io state machine is the canonical mutation-test target. Codex should weigh CI-time cost vs. coverage gain and recommend.

---

## Approval Checks (from skill template)

- **Worktree/branch creation**: pass — plan §2 step 14 makes worktree-first explicit; M0 first-commit exception is correct (empty repo, hook short-circuits).
- **Merge target explicit**: pass — `main` is named throughout; no implicit assumption.
- **Push approval**: pass — plan §15 item 16 makes every push approval-gated.
- **PR/MR approval**: pass — plan §15 item 20 (`wt merge -y`) covers it.
- **Issue creation approval**: not addressed in plan — *Fix*: add a §15 item 23: "Opening GitHub issues (e.g., the moonpool TLS upstream issue, the `cargo-fuzz` follow-up). Per-issue approval."

---

## Revised path (one-paragraph audited summary)

1. Surface the 12-item question list in §D to the user; collect answers.
2. Fold reviewer C-1, C-2, C-3, C-5, C-6, C-7, C-10 + audit §G items 1, 2, 4, 8, 9 + §E recommendations into a single in-place edit of `/home/florentin/.claude/plans/ask-quasar-plan.md`.
3. Run the optional `--with-codex` step with the 5 questions in §H.
4. After user answers, fold Q4, Q8, Q9, Q12 results into the plan (these are the question-derived edits).
5. Present the final plan to the user for sign-off; on approval, begin M0 by committing directly to `main` (empty-repo exception) and start the M1 worktree.

---

End of audit.
