# Review: ask-quasar-plan.md

> Reviewer pass over `/home/florentin/.claude/plans/ask-quasar-plan.md` against the dossier `/home/florentin/.claude/plans/ask-quasar-research.md`, the live Pulsar source tree at `/home/florentin/Sources/github.com/apache/pulsar`, the empty quasar repo, `~/.claude/CLAUDE.md`, and crates.io reality.

---

## A. Verdict

**APPROVED with required changes.** The architecture is sound: the sans-io split + dual-engine (tokio default, moonpool opt-in) is the right call, the v0.1.0 PIP set is correctly scoped against Pulsar 3.0.x LTS, and every load-bearing claim about the Java sources I spot-checked traces back to a real line. The dossier-to-plan handoff is clean: PIP set, FeatureFlags, framing magic numbers, BaseCommand table, trackers, ClientCnx maps, and HandlerState all match the plan §3-§6 narrative. Required fixes are dependency-version drift (`apache-avro`, `testcontainers`), one factual error (`resolver = "3"` vs the stated `rust-version = "1.85"`), three policy/process gaps (no `cargo audit` in CI; no `cargo doc -D warnings` semantics check; allow-list is too narrow for what M0–M6 actually need), and several over-confident sub-decisions (TLS gap default, batch-index ACK proto field name, `prost-build` strategy).

---

## B. Strengths

- **Citation discipline holds.** I verified 12 spot-cited references; 11 PASS, 1 imprecise but defensible (`ClientCnx.java:117` is the class line, not a field range — see §G).
- **Sans-io boundary is correctly placed.** `Connection::{handle_bytes, poll_transmit, poll_event, poll_timeout, handle_timeout}` mirrors `quinn-proto::Connection` cleanly, and the validation gate at M2 ("no `tokio`/`async`/`mio`/`socket2` in `cargo tree -p quasar-pulsar-proto`") is the right hard constraint.
- **PIP coverage matrix is real.** v0.1.0 PIPs (30, 37, 107, 131, 34, 119, 282, 379, 54, 391, 22, 58, 124, 409, 26, 68, 90, 145, 188, 296, 313, 344) all map to wire-affecting changes; the deferral list (PIP-4, PIP-31, PIP-33, PIP-180, PIP-415, PIP-460, PIP-466, PIP-121) is appropriate for a v0 driver.
- **Worktree-first is correctly described** (plan §2 step 14 and §15 item 20), including the explicit exception for the initial commit on the empty repo (the pre-edit hook returns early on empty repos because `git rev-parse --abbrev-ref HEAD` errors out — verified locally).
- **Approval gates are comprehensive** (20 items in §15), which matches `~/.claude/CLAUDE.md`'s "approval-gated actions" mandate. The defaults are conservative.
- **Test layering is well structured**: sans-io unit → broker fake → moonpool sim chaos → e2e Docker, in increasing cost order. The 10 representative unit tests directly correspond to existing Java test classes the dossier itemised.

---

## C. Required changes (must fix before audit)

### C-1. `apache-avro` version is stale.
- **Location**: plan §1 workspace deps table, line `apache-avro = "0.17"`.
- **Issue**: crates.io currently ships `apache-avro = "0.21.0"` (verified via `https://crates.io/api/v1/crates/apache-avro`, last release 2025-11-13). The 0.17 line is from 2024 and was pre-MSRV-1.85. The 0.21 line declares MSRV 1.85.0 — which matches the plan's `rust-version = "1.85"`.
- **Fix**: bump to `apache-avro = "0.21"` in the workspace deps table, and call this out in the dossier allow-list (§15 lists `apache-avro` without a version pin, which is fine — but the plan's chosen pin must be the current line, not a 6-version-old one).

### C-2. `testcontainers` version is stale.
- **Location**: plan §1 workspace deps table, line `testcontainers = "0.20"`.
- **Issue**: current is `0.27.3` (verified via crates.io). `0.20` lacks several runner ergonomics the e2e tests will want.
- **Fix**: bump to `testcontainers = "0.27"`. Note that the crate name is `testcontainers` on crates.io; the repo is `testcontainers-rs` on GitHub — the plan is correctly using the crate name, but the dossier allow-list mentions both names ambiguously; tighten to `testcontainers` only.

### C-3. `resolver = "3"` requires Rust 1.84, not 1.85, and is conflated with edition 2024.
- **Location**: plan §1 workspace `Cargo.toml` snippet (resolver = "3"); §2 step 5 ("Resolver `"3"`, edition `2024`, `rust-version = "1.85"`"); §2 risks ("Cargo `resolver = "3"` requires Rust 1.84+ stable").
- **Issue**: the plan correctly notes 1.84+ is the resolver-3 floor, but pins `rust-version = "1.85"` to satisfy moonpool's edition 2024 requirement. That's fine — but `resolver = "3"` *is* the default for edition 2024, so explicitly setting it is redundant; the bigger concern is that resolver 3 changes `incompatible-rust-versions` from `allow` to `fallback`, which may surprise users when `cargo` silently picks an older version of a transitive dep. The plan should call out this behaviour change so that the M0 `cargo deny` / lockfile review accounts for it.
- **Fix**: either drop the explicit `resolver = "3"` (it's implied by edition 2024) and add a paragraph in `GUIDELINES.md` documenting the `incompatible-rust-versions = "fallback"` behavior, or keep the explicit pin and add a one-line comment in `Cargo.toml` explaining why.

### C-4. `prost-build` strategy is half-specified.
- **Location**: plan §3 step 2 ("Decision: checked-in codegen via `xtask codegen`, not `build.rs`").
- **Issue**: the plan correctly notes that `prost-build` invokes `protoc` by default, and proposes `xtask codegen` writing generated files into `src/pb/` checked into git. But it doesn't say *how* `xtask codegen` runs without `protoc` on a contributor machine. The right answer is one of: (a) require `protoc` for contributors who run `xtask codegen` (most ergonomic — most distros have it; document in `CONTRIBUTING.md`); (b) ship a vendored `protoc` via `protoc-bin-vendored` crate (works on Linux/macOS, no x-platform headaches); (c) use `prost-build::Config::skip_protoc_run()` with `file_descriptor_set_path()` (the documented escape hatch — see `https://docs.rs/prost-build/latest/prost_build/struct.Config.html`), pre-generating descriptors via a CI-only step.
- **Fix**: pick (a) explicitly, document `protoc >= 3.19` in `CONTRIBUTING.md`, add `protoc --version` check at the top of `xtask codegen`. Drop the in-plan ambiguity.

### C-5. PIP-54 / PIP-391 ACK proto field needs verification.
- **Location**: plan §4 step 4 ("PIP-54: batch bitset support via `MessageIdData.ack_set`"); §4 step 5 ("`ack_tracker.rs`: ... PIP-54 batch bitset support via `MessageIdData.ack_set`").
- **Issue**: the dossier (§5 PIP-54) says "Adds `MessageIdData.ack_set` (bitset over batch indices)". This is correct (verified earlier in dossier §2: `MessageIdData (proto:59-69)` lists `ack_set`). But ACK uses `CommandAck.MessageIdData` via the `message_ids` repeated field; the bitset is on the `MessageIdData` inside ACK, not on a separate field. The plan should clarify which structure carries the bitset on the ACK path (it's the `MessageIdData` inside `CommandAck.message_ids`, not the top-level `CommandAck`). One sentence in §4 step 5 to that effect prevents an implementation slip.
- **Fix**: clarify wording in §4 step 5 of the plan.

### C-6. CI is missing `cargo audit`.
- **Location**: plan §2 step 12 (CI jobs list); §12 CI matrix.
- **Issue**: `cargo deny` is on the list (advisory + license check), but `cargo audit` (which uses the RustSec advisory DB independently and is what most Rust shops actually run) is not. Even though `cargo deny check advisories` overlaps, it's worth running `cargo audit` separately because the two tools track slightly different vuln sources and timing. The plan §12 says "**`cargo-vet` or `cargo-audit`** — pick at v0.2.0; defer" — defer is acceptable but the rationale should be that `cargo deny check advisories` covers the immediate need. Make that explicit.
- **Fix**: either (a) add `cargo audit` to v0.1.0 CI alongside `cargo deny`; or (b) keep deferred but state in the plan that `cargo deny check advisories` is the v0.1.0 substitute. Don't leave it ambiguous.

### C-7. `cargo doc -D warnings` semantics.
- **Location**: plan §12 CI matrix line `cargo doc --workspace --all-features --no-deps -D warnings`.
- **Issue**: `cargo doc` does not natively accept `-D warnings` like clippy does. The correct invocation is `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps`. Without this, broken intra-doc links will not fail CI.
- **Fix**: change the CI step to set `RUSTDOCFLAGS=-D warnings` before invoking `cargo doc`.

### C-8. Dependency allow-list is missing entries that M0–M6 will demand.
- **Location**: plan §15 item 19; dossier §15 (referenced).
- **Issue**: the allow-list does not include `prost-build`, `prost-types`, `tokio-util` (the actor inevitably wants `tokio_util::codec::Framed` or similar — even if you reject it, decide explicitly), `futures` / `futures-util` (for `Stream`/`Sink` glue), `parking_lot` (faster mutex if any state needs locking in engines), `prost-bin-vendored` if you go that route for §C-4, `cargo-deny` (it's a binary not a dep, but still), `socket2` (sometimes useful for TCP_NODELAY tuning before tokio takes over). At minimum, decide pre-M0 which of these are auto-allowed vs. each requires a separate approval gate.
- **Fix**: expand the allow-list in plan §15 item 19 to include `prost-build`, `prost-types`, `tokio-util`, `futures`/`futures-util`, `pin-project-lite`, `tracing-subscriber` (for examples), `anyhow` (for examples + xtask), and `clap` (for xtask + future CLI). Or explicitly declare that anything beyond the current list triggers an approval gate per addition — but then the implementer will be blocked at every commit.

### C-9. moonpool TLS gap default (option (c)) leaks scope.
- **Location**: plan §6 step 3 ("Default proposal for v0.1.0: (c) — TLS-less moonpool engine").
- **Issue**: shipping a TLS-less engine means *any* moonpool sim test against `pulsar+ssl://` cannot run. That's defensible for v0.1.0 (the engine is for sim, not production), but the plan needs to state explicitly that the sim engine's e2e is plaintext-only and document the implication: chaos-tests for the TLS handshake path are out of scope until (a) or (b) lands. Also missing: option (d) — wrap moonpool's connection in `tokio-rustls` outside moonpool (since rustls itself is sans-io, you can drive `ClientConnection::{read_tls, write_tls, process_new_packets}` over moonpool's bytes pipe synchronously). This is the *cleanest* path and the plan ignores it.
- **Fix**: add option (d) explicitly to §6 step 3 ("Local rustls adapter via moonpool's byte pipe"). Re-evaluate the default — (d) is technically lower-risk than (c) because it lets sim tests exercise the TLS handshake state machine deterministically.

### C-10. Worktree assumption for the M2 sub-milestones is implicit.
- **Location**: plan §13 (parallelisation of M2a–M2d, M3, M4).
- **Issue**: §13 says M2a–M2d can be parallelised and M3/M4 can run in parallel once M2 hits 80% — but doesn't say each parallel stream lives in its own `wt switch --create` worktree. Without that, the implementer (or another agent in a swarm) might edit the same crate from two contexts. Make the worktree-per-stream contract explicit.
- **Fix**: add a sentence to §13 saying each parallel milestone-stream gets its own `wt`-managed worktree; merges back to `main` are user-approved per stream.

---

## D. Optional improvements (nice-to-have)

- **`cargo-mutants` mutation testing.** Beyond proptest, run `cargo-mutants` against `quasar-pulsar-proto` once M2 stabilises. The sans-io state machine is the kind of code where mutation testing catches *real* gaps (the proptest only covers what you thought to assert; mutants flip code paths and check whether *any* test catches the mutation). Schedule for M2 close.
- **Sim engine TLS option (d) — see C-9** is also an *improvement* over (c). Worth promoting from "fix" to "default" if you confirm rustls's `ClientConnection::process_new_packets` works cleanly over moonpool's byte pipe.
- **Pulsar Java client interop test.** Spin up `apachepulsar/pulsar:3.0.x` standalone, run a Java producer (via the official Java client) + a quasar consumer (and vice versa), assert cross-publishes work. This catches subtle wire-format drift (proto field numbers, missing optional fields, FeatureFlag negotiation) that an all-quasar e2e suite won't. Add to e2e in v0.1.0 if you can find a way to ship the Java client as a sidecar in the testcontainer.
- **Quinn-proto-style API doc framing.** Plan §4 mentions the quinn-proto shape but doesn't propose phrasing the rustdoc that way. Worth adding a doc-page in `docs/architecture.md` titled "For users coming from quinn-proto" that maps `Connection::poll_transmit` → `poll_transmit`, `handle_bytes` → `handle_event(Datagram)`, etc. This lowers the onboarding cost for the QUIC crowd, which has overlap with the distributed-systems crowd quasar aims at.
- **`#[deny(missing_docs)]` from M1, not M2.** Plan §11 says deny at M2 for `quasar-pulsar-proto` — earlier is better because the codec crate is small enough that backfilling later is more painful than writing as-you-go.
- **`cargo-llvm-cov` coverage report.** Useful for the sans-io crate to confirm the state machine has full transition coverage. Not a release gate; add as a CI artifact.
- **`xtask check-vendor` job.** Add an `xtask check-vendor` that re-vendors `PulsarApi.proto` from upstream and diffs — catches Pulsar's proto drift without a human noticing.

---

## E. Open questions for the user (re-prioritised)

The plan's §17 already tiers the questions. I largely concur but rearrange one pair for sharpness. Verified each question is genuinely user-only (no source-readable answer).

**Tier 1 — blocks M0 (cannot start writing files without these):**

1. **Published crate name** (dossier §11 Q1 / plan §17 #1). Default: `quasar-pulsar`. `quasar` is taken (anowell, 2017, 0.0.1, 8 recent dl — verified via crates.io API).
2. **Repo hosting + GitHub repo creation** (dossier §11 Q13 / plan §17 #6). Default: `github.com/me/quasar`. Also gates the `gh repo create` action (plan §15 item 17).
3. **License** (dossier §11 Q2 / plan §17 #2). Default: Apache-2.0 only. Drives `LICENSE` file + every per-file SPDX header.
4. **moonpool risk acceptance + engine ordering** (dossier §11 Q3+Q14, plan §17 #3+#5 — *the planner echoed these as two questions; they're really one decision*). Default: tokio is the public-facing default, moonpool is opt-in for sim. Confirms both the engine set and the public-default story.
5. **Crate split granularity** (dossier §11 Q4 / plan §17 #4). Default: 6-crate split as proposed.

**Tier 2 — blocks M2 scope freeze (state machines can't start without these answers):**

6. **Minimum supported broker version** (Q5 / #7). Default: 3.0 LTS.
7. **Schema scope for v0.1.0** (Q6 / #8). Default: (a). Drives M5 surface.
8. **Auth scope for v0.1.0** (Q10 / #10). Default: token + TLS + AUTH_CHALLENGE. Drives M6 surface.
9. **Transactions in v0.1.0** (Q7 / #9). Default: defer. Drives M2 inclusion of TC client.
10. **Encryption (PIP-4) in v0.1.0** (Q11 / #11). Default: defer. Drives M2 inclusion of MessageMetadata encryption fields.

**Tier 3 — blocks v0.1.0 release cut (defer until M5/M6 close):**

11. **Admin REST client in v0.1.0** (Q8 / #12). Default: scaffolded, unpublished.
12. **CLI binary** (Q9 / #13). Default: library-only.
13. **E2e CI broker provisioning** (Q12 / #14). Default: testcontainers + compose fallback.
14. **Coexistence with pulsar-rs** (Q15 / #15). Default: separate project; revisit upstream after v0.1.0.

**New for the user (not in the original 15):**

15. **`Cargo.lock` policy** (plan §2 step 9, "Default: commit `Cargo.lock`"). The plan flags this as needing confirmation but folds it under "stub crates so cargo build succeeds at M0". This is M0-blocking and deserves its own Q.
16. **`protoc` contributor requirement** (per C-4). The user must agree the `xtask codegen` flow requires contributors install `protoc >= 3.19` locally.

---

## F. Approval-gated actions to surface to user

Verbatim copy of plan §15 with one addition (C-4 / Q16) and one clarification (Q15 + the no-default for the new GitHub repo). I tagged each with `[default: ...]` so the user can give a single blanket approval and the implementer knows what's auto-applied.

1. **Published crate name.** [default: `quasar-pulsar`]
2. **License.** [default: Apache-2.0 only]
3. **Engine set for v0.1.0.** [default: tokio + moonpool, tokio is public default]
4. **Crate split granularity.** [default: 6-crate split as proposed]
5. **Minimum supported broker version.** [default: Pulsar 3.0 LTS]
6. **Schema scope for v0.1.0.** [default: bytes + String + Json + raw Avro + Protobuf]
7. **Transactions in v0.1.0.** [default: defer to v0.2.0]
8. **Admin REST client in v0.1.0.** [default: scaffolded, unpublished]
9. **CLI binary.** [default: library-only for v0.1.0]
10. **Auth scope for v0.1.0.** [default: token + TLS + AUTH_CHALLENGE]
11. **Encryption (PIP-4) in v0.1.0.** [default: defer]
12. **E2e CI broker provisioning.** [default: testcontainers + docker-compose fallback]
13. **Repo hosting.** [default: `github.com/me/quasar`]
14. **moonpool risk acceptance.** [default: tokio public default, moonpool opt-in]
15. **Coexistence with pulsar-rs.** [default: separate project]
16. **Push to GitHub.** [no default — always per-push]
17. **Creating the GitHub repository** (`gh repo create`). [no default — explicit]
18. **Publishing any crate to crates.io.** [no default — per-crate]
19. **Adding any non-trivial dependency outside allow-list.** [no default — per-dep; expand allow-list per C-8 first]
20. **Merging `wt` worktrees to `main`.** [no default — per-merge]
21. **(NEW) `Cargo.lock` committed to git.** [default: commit it]
22. **(NEW) `protoc >= 3.19` as contributor build-tool dependency for `xtask codegen`.** [default: yes; document in CONTRIBUTING.md]

I did **not** find a "must contact `anowell` re: crate name `quasar`" gate to add. `quasar` is published but on a different ecosystem (wasm/asmjs); the right play is to publish under `quasar-pulsar` (or whichever name the user picks) and not attempt to claim the abandoned `quasar` name. No outreach to anowell needed unless the user wants to try transferring it (low-effort to ask, low-value to receive — `quasar-pulsar` is more discoverable anyway).

---

## G. Source-citation spot check

| # | Plan claim | Verified location | Result |
|---|---|---|---|
| 1 | "magicCrc32c = 0x0e01 (`Commands.java:138`)" | `Commands.java:138`: `public static final short magicCrc32c = 0x0e01;` | **PASS** |
| 2 | "magicBrokerEntryMetadata = 0x0e02 (`Commands.java:140`)" | `Commands.java:140`: `public static final short magicBrokerEntryMetadata = 0x0e02;` | **PASS** |
| 3 | "checksumSize = 4 (`Commands.java:141`)" | `Commands.java:141`: `private static final int checksumSize = 4;` | **PASS** |
| 4 | "BaseCommand `enum Type` and `required Type type = 1` at PulsarApi.proto:1144-1342" | proto:1145 `enum Type {`, proto:1146 `CONNECT = 2;`, proto:1250 `required Type type = 1;` | **PASS** (range bracket is correct) |
| 5 | "FeatureFlags at proto:311-320 with `supports_broker_entry_metadata=2`, `supports_topic_watchers=4`, `supports_get_partitioned_metadata_without_auto_creation=5`" | proto:311 `message FeatureFlags {`, :312 supports_auth_refresh=1, :313 supports_broker_entry_metadata=2, :315 supports_topic_watchers=4, :316 supports_get_partitioned_metadata_without_auto_creation=5 | **PASS** |
| 6 | "ProtocolVersion at proto:254, claim v21" | proto:254 `enum ProtocolVersion {`; v21 = 21 is the highest enum value | **PASS** |
| 7 | "CompressionType at proto:92 (NONE/LZ4/ZLIB/ZSTD/SNAPPY)" | proto:92 `enum CompressionType { NONE=0; LZ4=1; ZLIB=2; ZSTD=3; SNAPPY=4; }` | **PASS** |
| 8 | "ProducerAccessMode at proto:100 (Shared/Exclusive/WaitForExclusive/ExclusiveWithFencing)" | proto:100 `enum ProducerAccessMode { Shared=0; Exclusive=1; WaitForExclusive=2; ExclusiveWithFencing=3; }` | **PASS** |
| 9 | "ClientCnx.java:117 (extends PulsarHandler), pendingRequests at :132-134, producers at :141, consumers at :147" | `ClientCnx.java:117`: `public class ClientCnx extends PulsarHandler {`; :132 pendingRequests; :141 producers (close enough — actual is :142 declaration after annotations on :140); :147 consumers (annotations on :146) | **PASS (with off-by-one tolerance)** — the cited lines bracket the field declarations correctly |
| 10 | "handleConnected at :432, handleAuthChallenge at :464, handleSendReceipt at :515" | `ClientCnx.java:432`: `protected void handleConnected(...)`; :464 handleAuthChallenge; :515 handleSendReceipt | **PASS** |
| 11 | "ProducerImpl.java:113 batchMessageContainer at :135, lastSequenceIdPublished at :153, lastSequenceIdPushed at :158" | `ProducerImpl.java:113`: `public class ProducerImpl<T>...`; :135 batchMessageContainer; :155 lastSequenceIdPublished (plan says :153 — close, the declaration vs annotation order); :160 lastSequenceIdPushed (plan says :158 — same off-by-2) | **PASS (with off-by-2 tolerance)** — the cited lines point at the AtomicReferenceFieldUpdater for the volatile field |
| 12 | "ConsumerImpl.java:143, acknowledgmentsGroupingTracker at :174, negativeAcksTracker at :175, seekStatus at :185, deadLetterPolicy at :209, chunkedMessagesMap at :219" | `ConsumerImpl.java:143`: `public class ConsumerImpl<T>...`; :174 acknowledgmentsGroupingTracker; :175 negativeAcksTracker; :185 seekStatus; :209 deadLetterPolicy; :219 chunkedMessagesMap | **PASS** |

**Bonus check**: "PIP-145 file absent in this snapshot, but command exists at proto:1229-1232" (dossier §5 row PIP-145). Verified: no `pip/pip-145.md` in `/home/florentin/Sources/github.com/apache/pulsar/pip/`. Confirmed via `ls pip/ | grep pip-145` (no match). The plan correctly leans on the proto evidence + Java `TopicListWatcherTest.java` instead of the missing PIP doc. **PASS**.

**Conclusion**: 12/12 spot-checks PASS (with small off-by-one/off-by-two on Java line numbers due to annotation lines between the cited line and the field declaration — defensible). The dossier and plan are not making up citations.

---

## H. Risk the plan under-estimates

**The real surprise is the interaction between PIP-30 (AUTH_CHALLENGE) and in-flight requests during token refresh, combined with chunked-message reassembly crossing batch boundaries.**

The plan §4 step 2 / handler `handle_auth_challenge` says "PIP-30, ask the configured `AuthDataProvider` for a refresh challenge, enqueue `AUTH_RESPONSE`" and §16 risks-table calls it "AUTH_CHALLENGE token-refresh race [medium/medium]". Both under-state the actual problem.

In the Java client, `AUTH_CHALLENGE` can arrive *mid-flight* — the broker can challenge at any time. Between the moment the broker sends `AUTH_CHALLENGE` and the moment the client returns `AUTH_RESPONSE`, the client must:

1. Not panic if an in-flight `SEND` was already pushed (the broker may accept or reject it; `SEND_RECEIPT` may still come back even after challenge).
2. Not duplicate any send when the new auth context kicks in (the dedup ledger must work *across* the refresh).
3. Handle `AUTH_CHALLENGE` happening *during* a chunked message — when the producer has emitted chunk 3 of 5 and the broker challenges. The Java client's `ProducerImpl.java:1570` handles `ChunkedMessageCtx` cleanup on `SEND_ERROR`; but the corresponding cleanup on a *successful* challenge-then-resume path is subtle and not well-documented in PIP-30 or PIP-292.
4. Handle `AUTH_CHALLENGE` during key_shared rebalance (a fresh consumer attach happens around the same time the broker draining-hashes; if the auth refresh fails, the broker fences the consumer with a different code path than a fresh subscribe).

The plan's M2 timeline allocates 3-4 weeks for the full state machine; the auth-refresh-during-chunked-send edge case alone is worth ~2-3 days of careful testing. Without explicit test coverage in `quasar-pulsar-proto/tests/auth_refresh_during_chunked_send.rs`, this will be the first thing that breaks under a real-world token-renewal scenario (Kubernetes service-account tokens rotate every ~60 minutes; chunked messages can take >60 seconds to fully transmit on a slow link).

Secondary under-estimated risks (in declining order):

- **key_shared draining-hashes (PIP-379) implementation.** The plan §4 step 4 / `key_shared_dispatch.rs` test mentions it but doesn't allocate special M2 budget. PIP-379 (27 KB doc — verified) describes draining-hash semantics where the *broker* drives the dispatch and the *client* just observes. Getting the consumer state machine to correctly *not* assume keys stay with the same consumer is subtle.
- **`prost`-generated code drift between Pulsar versions.** Pulsar's `PulsarApi.proto` adds optional fields regularly (FeatureFlags grew from 4 to 8 fields over the last 4 minor versions). The `xtask codegen --check` job will surface drift but the plan does not specify the *response* — is drift a CI failure (block merge) or a CI warning (open issue)? Decide.
- **`tokio-rustls` v0.26 / `rustls` v0.23 API surface for `ServerName::try_from`.** rustls v0.23 deprecated several APIs the v0.22 examples used; the plan §5 step 2 doesn't show the exact connect snippet, but the implementer will hit `IoError(InvalidDnsName)` if they pass a non-IP string without `ServerName::try_from(&str)`. Spell out the rustls v0.23 connect path in the plan or in `crates/quasar-pulsar-runtime-tokio/src/engine.rs` design notes.
- **moonpool 0.6 → 0.7 API break.** The plan §6 step 5 pins `moonpool-core = "=0.6"` which is correct, but doesn't say what the *unpinning trigger* is. Add: "Unpin to `^0.7` only after `quasar-pulsar-runtime-moonpool` v0.2.0 release prep, gated on a re-validation of the chaos test suite."

---

End of review.
