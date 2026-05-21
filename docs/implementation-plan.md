# Implementation Plan: magnetar — Apache Pulsar sans-io Rust Driver

> Single comprehensive plan for the empty repo at `/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar`.
> Companion documents (in `/home/florentin/.claude/plans/`):
> - `ask-magnetar-research.md` — research dossier (cited as "research §N")
> - `ask-magnetar-review.md` — reviewer report
> - `ask-magnetar-audit.md` — audit verdict
> - `ask-magnetar-codex.md` — Codex cross-check
> - `ask-magnetar-decisions.md` — **AUTHORITATIVE decision log signed off by Florentin 2026-05-20. Overrides defaults in this plan where they conflict.**
>
> When this plan and `ask-magnetar-decisions.md` disagree, the decisions doc wins. Examples: project name `magnetar` (not `quasar-pulsar`), v0.1.0 scope is full Java parity (no deferrals), minimum broker is Pulsar 4.0 (not 3.0 LTS), moonpool TLS via option (d), **no channels anywhere**, no pulsar-rs migration content.

---

## 0. Executive summary

- **Goal.** Build `magnetar`, a new Apache Pulsar driver in Rust, with a **sans-io protocol core** (quinn-proto shape + handle façade) and **multiple swappable I/O engines** (tokio public default, moonpool opt-in for deterministic simulation). v0.1.0 ships **feature-complete relative to the Java client** on a Pulsar 4.0+ broker — nothing deferred to a later release. Hosted at `/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar`.
- **Architectural choice.** `magnetar-proto` exposes a sans-io `Connection` state machine; engines (`magnetar-runtime-tokio`, `magnetar-runtime-moonpool`) feed bytes in/out and drive timers. The protocol crate has zero I/O dependencies. **No channels** anywhere in the workspace (per the decision log): `Arc<parking_lot::Mutex<Connection>>` + `tokio::sync::Notify` + in-state `Waker` slabs replace mpsc.
- **v0.1.0 PIP scope** (per decisions §"v0.1.0 scope"). IN: PIP-30, 37, 107, 131, 34, 119, 282, 379, 54, 391, 22, 58, 124, 409, 26, 68, 90, 145, 188, 296, 313, 344, **31 (transactions), 4 (encryption), 33 (replicated subs), 121 (cluster failover), 180 (shadow topic), 415 (getMessageIdByIndex), 460/466 (scalable topics, experimental tag)**. No PIP deferred. CLI + admin REST both ship in v0.1.0.
- **Key risks.** moonpool maturity ("hobby-grade" per its own README — research §6); crate name `quasar` is taken on crates.io but irrelevant now (we use `magnetar` — research §9); TLS over moonpool resolved via option (d): local `rustls` adapter over moonpool's byte pipe (rustls is itself sans-io).
- **Timeline shape.** Ten phased milestones M0–M9 (bootstrap, codec, sans-io state machine, tokio engine, moonpool engine, schemas full parity, auth full parity, transactions, encryption, admin+CLI+remaining PIPs) with v0.1.0 cut after M9. This is a multi-month effort.
- **Final crate set.** `magnetar` (façade), `magnetar-proto` (sans-io), `magnetar-runtime-tokio`, `magnetar-runtime-moonpool`, `magnetar-admin` (REST), `magnetar-cli` (binary), `magnetar-fakes` (dev-dep broker fake), `magnetar-auth-oauth2`, `magnetar-auth-sasl`, `magnetar-auth-athenz`, `magnetar-messagecrypto` (PIP-4). License Apache-2.0 only. Edition 2024, MSRV 1.85.

---

## 1. Crate topology

### Diagram

```
+-------------------------------------------------------------+
|                       quasar-pulsar                          |  <- public façade (re-exports + builder)
|  (feature gates: tokio, moonpool, schema-extras, auth-...)   |
+--------------------------+----------------------------------+
                           |
       +-------------------+-------------------+
       |                                       |
+------v---------------+        +--------------v----------------+
| quasar-pulsar-       |        | quasar-pulsar-                |
| runtime-tokio        |        | runtime-moonpool              |
| (TCP + tokio-rustls) |        | (moonpool-core providers)     |
+------+---------------+        +--------------+----------------+
       |                                       |
       +-------------------+-------------------+
                           |
                  +--------v---------+
                  | quasar-pulsar-   |  <- sans-io core (zero I/O deps)
                  | proto            |     Connection / Producer / Consumer
                  |                  |     state machines + codec
                  +--------+---------+
                           |
                  +--------v---------+
                  | quasar-pulsar-   |  <- dev-dep only: in-process broker fake
                  | fakes            |     for sans-io unit tests
                  +------------------+

(siblings, not in the dep tree of the core driver)
+----------------------+        +----------------------------+
| quasar-pulsar-admin  |        | xtask                      |
| (REST, reqwest)      |        | (e2e launcher, codegen)    |
+----------------------+        +----------------------------+
```

### Crate table

| Crate | Role | Depends on | Public API (one-liner) | Sans-io / engine |
|---|---|---|---|---|
| `quasar-pulsar-proto` | Pulsar wire codec + `Connection`/`Producer`/`Consumer` state machines | `prost`, `bytes`, `crc32c`, `lz4_flex`, `zstd`, `snap`, `flate2`, `thiserror`, `tracing` | `Connection::{new, handle_bytes, poll_transmit, poll_event, poll_timeout, handle_timeout, open_producer, subscribe, send, ack, ...}` | sans-io |
| `quasar-pulsar` | Public façade; builder; re-exports; engine selection | `quasar-pulsar-proto`, optional `quasar-pulsar-runtime-tokio` / `-runtime-moonpool` (feature-gated) | `PulsarClient::builder()...build()`; `Producer`, `Consumer`, `Reader`, `Schema` traits | thin glue |
| `quasar-pulsar-runtime-tokio` | Tokio engine | `tokio`, `tokio-rustls`, `rustls`, `rustls-pemfile`, `quasar-pulsar-proto` | `TokioEngine::new(config) -> impl Engine` | engine |
| `quasar-pulsar-runtime-moonpool` | Moonpool engine | `moonpool-core`, `quasar-pulsar-proto`, optional `rustls` (see §6) | `MoonpoolEngine::new(providers) -> impl Engine` | engine |
| `quasar-pulsar-admin` | Pulsar REST admin client | `reqwest`, `serde`, `quasar-pulsar-proto` (for shared types) | `AdminClient::namespaces()`, `topics()`, etc. (post-v0.1.0) | engine (tokio-only) |
| `quasar-pulsar-fakes` | In-process sans-io broker for unit tests | `quasar-pulsar-proto` | `FakeBroker::new().on_subscribe(...).on_send(...)` | sans-io (dev-dep) |
| `xtask` | Build helpers (e2e launcher, codegen runner, license header check) | unpublished | `cargo xtask e2e`, `cargo xtask codegen` | unpublished |

### Workspace `Cargo.toml` snippet (target)

```toml
[workspace]
resolver = "3"
members  = [
    "crates/quasar-pulsar-proto",
    "crates/quasar-pulsar",
    "crates/quasar-pulsar-runtime-tokio",
    "crates/quasar-pulsar-runtime-moonpool",
    "crates/quasar-pulsar-admin",
    "crates/quasar-pulsar-fakes",
    "xtask",
]

[workspace.package]
edition      = "2024"
rust-version = "1.85"
license      = "Apache-2.0"
repository   = "https://github.com/<owner>/quasar"
authors      = ["Florentin Dubois <florentin.dubois@clever-cloud.com>"]

[workspace.dependencies]
# sans-io core
prost      = "0.13"
bytes      = "1.7"
crc32c     = "0.6"
lz4_flex   = "0.11"
zstd       = "0.13"
snap       = "1.1"
flate2     = "1.0"
thiserror  = "1"
tracing    = "0.1"
# tokio engine
tokio          = { version = "1", features = ["rt-multi-thread", "net", "io-util", "macros", "time", "sync"] }
tokio-rustls   = "0.26"
rustls         = "0.23"
rustls-pemfile = "2"
# moonpool engine
moonpool-core  = "0.6"
# schemas
serde       = { version = "1", features = ["derive"] }
serde_json  = "1"
apache-avro = "0.21"   # MSRV 1.85; matches our edition 2024 (reviewer C-1)
# admin
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json"] }
# tests
testcontainers = "0.27" # 0.27.3 current (reviewer C-2)
rstest         = "0.21"
proptest       = "1"

[profile.release]
lto = "thin"
codegen-units = 1
```

Note: cite versions on the dossier allow-list (research §15). Anything else is approval-gated.

---

## 2. Workspace bootstrap (Milestone M0)

### Context

- The repo is empty (research §9): on `main`, **zero commits**, no `Cargo.toml`, no `src/`, no LICENSE, no README, no CI.
- The pre-edit hook `~/.claude/hooks/pre-edit-default-branch.sh` blocks edits on `main` for *existing* repos; an empty repo has no default-branch state to protect, so the **first commit goes directly to `main`** (research §9). All later work uses `wt switch --create <branch> -y`.
- Approval-gated decisions (license, name) should be confirmed **before** running M0. Defaults below are placeholders pending sign-off.

### Steps

1. **Confirm approval-gated decisions** (do NOT proceed without explicit user OK):
   - Published crate name. Default: `quasar-pulsar` (research §11 Q1).
   - License. Default: `Apache-2.0` (matches Pulsar upstream and moonpool — research §11 Q2).
   - Repo hosting. Default: `github.com/me/quasar` (Florentin's personal — research §11 Q13).
   - moonpool risk acceptance. Default: tokio is the public default; moonpool is opt-in (research §11 Q14).

2. **`LICENSE`** at repo root. Apache-2.0 boilerplate from <https://www.apache.org/licenses/LICENSE-2.0.txt>.
   - File: `/home/florentin/Sources/github.com/me/quasar/LICENSE`.
   - Validation: file is byte-identical to the canonical Apache-2.0 text.

3. **`NOTICE`** at repo root.
   - Content: `quasar-pulsar\nCopyright 2026 Florentin Dubois\n\nThis product includes software developed at Clever Cloud (https://clever-cloud.com).\n`.

4. **`README.md`** at repo root.
   - One paragraph: what quasar is (sans-io Pulsar driver), the architectural choice (proto crate + tokio/moonpool engines), status banner ("**Pre-release. Wire protocol implemented for Pulsar 3.0+, see GUIDELINES.md for the supported-PIP matrix.**"), build / test snippet, license line.

5. **`Cargo.toml`** workspace at repo root. Use the snippet in §1.
   - Resolver `"3"`, edition `2024`, `rust-version = "1.85"` (matches moonpool edition 2024 — research §6).
   - `members = [...]` reflecting §1 topology.
   - `workspace.dependencies` lists allow-list deps only.
   - Validation: `cargo metadata --no-deps` succeeds; `cargo build --workspace` succeeds (after stub crates exist, see step 8).

6. **`rustfmt.toml`** at repo root.
   - Pin: `edition = "2024"`, `imports_granularity = "Module"`, `group_imports = "StdExternalCrate"`, `reorder_imports = true`, `use_field_init_shorthand = true`, `max_width = 100`.
   - Validation: `cargo +nightly fmt --check` passes.

7. **`clippy.toml`** and **`deny.toml`** at repo root.
   - `clippy.toml`: `msrv = "1.85"`, `cognitive-complexity-threshold = 30`, `too-many-arguments-threshold = 8`.
   - `deny.toml`: drives `cargo deny` (license allow-list, advisory checks).

8. **`rust-toolchain.toml`** at repo root.
   - `[toolchain]\nchannel = "stable"\ncomponents = ["clippy", "rustfmt"]\n`. Nightly is invoked via `cargo +nightly fmt` per the user's CLAUDE.md.

9. **`.gitignore`** at repo root: `/target`, `/Cargo.lock` (workspaces sometimes commit it — see decision below), `*.swp`, `.direnv/`, `.envrc.local`, `.idea/`, `.vscode/`, `.DS_Store`, `*.profraw`.
   - **Cargo.lock policy**: this is a library workspace **and** publishes binaries (xtask, future CLI). Default: **commit `Cargo.lock`** (catches deps regressions in CI). Confirm with user.

10. **`.envrc`** at repo root with `use mise` line for mise + direnv (matches user's environment in CLAUDE.md). Hook surfaces direnv state via `SessionStart`.

11. **Stub crates** so `cargo build` succeeds at M0:
    - `crates/quasar-pulsar-proto/Cargo.toml` + `src/lib.rs` with a doc comment and a single `pub struct Connection;` placeholder.
    - `crates/quasar-pulsar/Cargo.toml` + `src/lib.rs` re-exporting `quasar_pulsar_proto`.
    - `crates/quasar-pulsar-runtime-tokio/Cargo.toml` + `src/lib.rs` (`pub struct TokioEngine;`).
    - `crates/quasar-pulsar-runtime-moonpool/Cargo.toml` + `src/lib.rs` (`pub struct MoonpoolEngine;`).
    - `crates/quasar-pulsar-admin/Cargo.toml` + `src/lib.rs` (`//! Admin REST client. v0.2.0+.`).
    - `crates/quasar-pulsar-fakes/Cargo.toml` + `src/lib.rs` (`//! In-process broker fake.`).
    - `xtask/Cargo.toml` + `src/main.rs` with a no-op `main`.

12. **CI workflow** `.github/workflows/ci.yml`:
    - Trigger: `push`, `pull_request`.
    - Matrix: `os: [ubuntu-latest]`, `rust: [stable, nightly]`. Nightly used only for `cargo +nightly fmt --check`.
    - Jobs (in order, each fail-fast): `fmt-check` (nightly), `clippy -D warnings` (stable, `--all-features`), `build --all-features`, `test --workspace --all-features`, `doc --no-deps --all-features`.
    - Separate job `e2e` (manual trigger via `workflow_dispatch` and on push to release branches): runs `cargo test --features e2e` inside an Ubuntu runner with Docker.
    - Job `cargo deny` (advisory + license check).
    - Caching: `Swatinem/rust-cache@v2`.

13. **`GUIDELINES.md`** at repo root — see §11 below for full content. Created at M0 so subsequent work follows the rules.

14. **`AGENTS.md`** at repo root — a one-page agent instructions file:
    - "Default branch: `main`. All non-bootstrap work uses `wt switch --create <branch> -y` (research §10)."
    - "Commits: conventional + signed (`git commit -s -S`)."
    - "No `Co-Authored-By: Claude` / `Generated by Claude` trailers."
    - "Validation chain: `cargo build --all-features && cargo clippy --all-features -- -D warnings && cargo +nightly fmt && cargo test --workspace`."
    - Reference `GUIDELINES.md` for protocol-correctness rules.

15. **Initial commit on `main`** (the *only* commit allowed directly on `main`):
    - `git add -A && git commit -s -S -m "chore: bootstrap quasar workspace"`. Per user's CLAUDE.md, conventional + signed.
    - Validation: `cargo build --workspace --all-features && cargo clippy --workspace --all-features -- -D warnings && cargo +nightly fmt --check && cargo test --workspace`.

16. **Push to GitHub** — approval-gated. The repo on GitHub may not exist yet; if so, that creation is also gated (research §11 Q13).

17. **Docs to update at M0**: `README.md` (new), `GUIDELINES.md` (new), `AGENTS.md` (new), `LICENSE` (new), `NOTICE` (new). No public API yet, so no rustdoc obligations.

### Risks for M0

- Cargo `resolver = "3"` is **edition-2024 default** — explicit pin is redundant but kept to document intent; document in `GUIDELINES.md` that this silently switches `incompatible-rust-versions` from `allow` to `fallback` (reviewer C-3). Verify with `cargo --version` ≥ 1.85 before commit.
- moonpool requires edition 2024 (research §6) — `rust-version = "1.85"` is the binding constraint.
- `cargo deny` may flag transitive licenses; tune `deny.toml` allow-list iteratively before unblocking CI.

---

## 3. Codec layer (Milestone M1, in `quasar-pulsar-proto`)

### Context

- Pulsar's wire format is two framings (research §2):
  - **Simple command frame** (no payload): `[TOTAL_SIZE u32][CMD_SIZE u32][BaseCommand]`. (`Commands.java:1866-1883`.)
  - **Send/Message frame** (with payload): `[TOTAL_SIZE u32][CMD_SIZE u32][BaseCommand][MAGIC u16=0x0e01][CRC32C u32][METADATA_SIZE u32][MessageMetadata][PAYLOAD]`. (`Commands.java:1885-1934`.)
  - Optional **broker-entry metadata** envelope prepended on dispatch: `[MAGIC u16=0x0e02][BEM_SIZE u32][BrokerEntryMetadata]`. Detect by `getShort(readerIndex) == 0x0e02` and skip/parse before the standard frame. (`Commands.java:138-141`, `:1974-2038`.)
- CRC32C polynomial is Castagnoli (matches Java's `io.netty.handler.codec.compression.snappy.Crc32C` / SSE4.2 `_mm_crc32_u64`). Use the `crc32c` crate (research §16).
- BaseCommand is one tagged union with `required Type type = 1`; the type-id table is research §2 (`PulsarApi.proto:1144-1342`).

### Steps

1. **Vendor `PulsarApi.proto`** from `/home/florentin/Sources/github.com/apache/pulsar/pulsar-common/src/main/proto/PulsarApi.proto` into `crates/quasar-pulsar-proto/proto/PulsarApi.proto`.
   - Also vendor `PulsarMarkers.proto` (`pulsar-common/src/main/proto/PulsarMarkers.proto`) for marker types referenced at `MessageMetadata.marker_type` (research §2).
   - Add a `proto/README.md` documenting the source commit hash of the vendored file (record from `git -C /home/florentin/Sources/github.com/apache/pulsar log -1 --format=%H pulsar-common/src/main/proto/PulsarApi.proto` at vendor time).
   - Validation: `cargo build` after `build.rs` runs (next step).

2. **`build.rs`** in `quasar-pulsar-proto` using **`prost-build`**:
   - Decision: **`prost`** (not `quick-protobuf`). Justification: prost is the established choice in tokio/tonic ecosystem; debugging tooling is better; we don't need `no_std`. `quick-protobuf` saves a few hundred KB but adds friction. (Allow-list per research §15.)
   - `prost-build::Config::new().out_dir("src/pb").compile_protos(&["proto/PulsarApi.proto", "proto/PulsarMarkers.proto"], &["proto/"])`.
   - Emit into `src/pb/` so the generated code is **checked into git** (avoids requiring `protoc` for downstream users): use `prost_build::Config::compile_protos_into_string` and write via `xtask codegen` instead of `build.rs`. **Decision: checked-in codegen via `xtask codegen`**, not `build.rs`. Justification: no `protoc` requirement on consumer machines; CI re-runs `xtask codegen` and diffs to catch drift.
   - `crates/quasar-pulsar-proto/src/lib.rs`: `pub mod pb { include!("pb/pulsar.proto.rs"); }`.
   - Validation: `cargo build -p quasar-pulsar-proto` succeeds; `xtask codegen --check` produces no diff.

3. **Framing module** `crates/quasar-pulsar-proto/src/frame.rs`:
   - `pub fn encode_command(buf: &mut BytesMut, cmd: &pb::BaseCommand)` — emits the simple frame; `total_size = cmd_size + 4`, big-endian.
   - `pub fn encode_send(buf: &mut BytesMut, cmd: &pb::BaseCommand, metadata: &pb::MessageMetadata, payload: &[u8])` — emits the send/message frame with magic + CRC32C over `[METADATA_SIZE][METADATA][PAYLOAD]`.
   - `pub fn decode(src: &mut BytesMut) -> Result<Option<Frame>, FrameError>` — pulls a full frame off `src` or returns `Ok(None)` if incomplete. `Frame` is an enum:
     ```text
     pub enum Frame {
         Command(pb::BaseCommand),
         Message { cmd: pb::BaseCommand, metadata: pb::MessageMetadata, payload: Bytes, broker_entry: Option<pb::BrokerEntryMetadata> },
     }
     ```
   - Magic-byte guard: peek `u16` at offset 0 — `0x0e02` → consume broker-entry envelope, then decode the message frame; `0x0e01` after the command → CRC32C-verified message frame.
   - CRC32C verify on decode; mismatch → `FrameError::ChecksumMismatch` (maps to Pulsar's `CommandAck.ValidationError::ChecksumMismatch` — research §2).
   - Use `bytes::BytesMut::split_to` to avoid copies; payload returned as `bytes::Bytes`.
   - Validation: roundtrip test per BaseCommand type (research §4 / `BatchMessageContainerImplTest.java` is the inspiration).

4. **Compression** module `crates/quasar-pulsar-proto/src/compression.rs`:
   - `pub enum CompressionType { None, Lz4, Zlib, Zstd, Snappy }` (matches proto `CompressionType` at proto:92).
   - `pub fn compress(typ: CompressionType, payload: &[u8]) -> Bytes`.
   - `pub fn decompress(typ: CompressionType, payload: &[u8], uncompressed_size: u32) -> Result<Bytes, CompressionError>`.
   - Backends: `lz4_flex`, `flate2` (zlib), `zstd`, `snap` — all on allow-list (research §15).
   - Validation: roundtrip property test per compression type.

5. **Crate-internal error type** `crates/quasar-pulsar-proto/src/error.rs`:
   - `pub enum ProtoError { Framing(FrameError), Decode(prost::DecodeError), Compression(CompressionError), Broker(pb::ServerError, String), ... }`. `thiserror` derive.
   - **No panics** in this crate beyond invariant violations explicitly documented in `GUIDELINES.md`.

6. **Tests** (`crates/quasar-pulsar-proto/tests/codec.rs`):
   - `roundtrip_command_<type>` for each of the 70 BaseCommand types in research §2 (table-driven; one `#[rstest]` per type).
   - `decode_partial_frame_returns_none` (split frame across two `handle_bytes` calls).
   - `decode_broker_entry_metadata_prefix` — feed `[0x0e02][BEM][standard frame]`, assert decoder peels both layers.
   - `decode_checksum_mismatch_returns_error`.
   - Compression roundtrip per `CompressionType`.

### Docs to update at M1

- `crates/quasar-pulsar-proto/README.md`: codec scope + supported BaseCommand list.
- `docs/protocol.md` (new): high-level wire-format diagram + link to Pulsar's spec at <https://pulsar.apache.org/docs/3.0.x/developing-binary-protocol/>.

### Validation gate for M1

`cargo build -p quasar-pulsar-proto --all-features && cargo clippy -p quasar-pulsar-proto --all-features -- -D warnings && cargo test -p quasar-pulsar-proto`.

---

## 4. Sans-io state machine (Milestone M2, in `quasar-pulsar-proto`)

### Context

- This is the biggest crate. It mirrors the Java client's `ClientCnx`, `ProducerImpl`, `ConsumerImpl`, and all trackers — but in the **quinn-proto shape**: bytes in, bytes/events out, deterministic time. (Research §3, §7.)
- API target (research §7):
  ```text
  pub struct Connection { /* ... */ }
  impl Connection {
      pub fn new(config: ConnectionConfig) -> Self;
      pub fn handle_bytes(&mut self, now: Instant, bytes: &[u8]);
      pub fn poll_transmit(&mut self, buf: &mut Vec<u8>) -> Option<usize>;
      pub fn poll_event(&mut self) -> Option<ConnectionEvent>;
      pub fn poll_timeout(&self) -> Option<Instant>;
      pub fn handle_timeout(&mut self, now: Instant);
      pub fn open_producer(...) -> ProducerHandle;
      pub fn subscribe(...) -> ConsumerHandle;
      pub fn send(h: ProducerHandle, msg: OutgoingMessage);
      pub fn ack(h: ConsumerHandle, ack: Ack);
  }
  ```

### Step-by-step

1. **`HandlerState`** at `crates/quasar-pulsar-proto/src/handler_state.rs`.
   - Direct port of `pulsar-client/src/main/java/org/apache/pulsar/client/impl/HandlerState.java:27-105`.
   - `enum State { Uninitialized, Connecting, Ready, Closing, Closed, Terminated, Failed, RegisteringSchema, ProducerFenced }`. Pure transitions.
   - Tests: state-transition matrix.

2. **`Connection`** at `crates/quasar-pulsar-proto/src/connection.rs`.
   - Mirrors `ClientCnx.java:117` (research §3).
   - State:
     - `handler_state: HandlerState`,
     - `pending_requests: HashMap<u64, RequestKind>` (keyed by request-id, mirrors `ClientCnx.java:132-134`),
     - `producers: BTreeMap<u64, ProducerState>` (keyed by producerId, mirrors `:141`),
     - `consumers: BTreeMap<u64, ConsumerState>` (keyed by consumerId, mirrors `:147`),
     - `next_request_id: u64`, `next_producer_id: u64`, `next_consumer_id: u64`, `next_sequence_id: u64`,
     - `in_buf: BytesMut`, `out_buf: VecDeque<Bytes>`,
     - `ping_interval: Duration`, `next_ping_at: Option<Instant>`,
     - `feature_flags_negotiated: pb::FeatureFlags` (set on CONNECTED),
     - `protocol_version_claimed: i32 = ProtocolVersion::V21 as i32`.
   - Methods:
     - `handle_bytes(now, bytes)`: append to `in_buf`, loop `frame::decode`, dispatch per BaseCommand type via match.
     - `poll_transmit(buf) -> Option<usize>`: pop from `out_buf`, copy into `buf`, return length.
     - `poll_event() -> Option<ConnectionEvent>`: pop from internal event queue.
     - `poll_timeout() -> Option<Instant>`: `min(next_ping_at, trackers' next_deadlines...)`.
     - `handle_timeout(now)`: tick pings + per-tracker pollers.
   - Handlers (mirror Java method names for grep parity):
     - `handle_connected` (Java `ClientCnx.java:432`) — capture server protocol version, FeatureFlags, transition to Ready.
     - `handle_auth_challenge` (Java `:464`) — PIP-30, ask the configured `AuthDataProvider` for a refresh challenge, enqueue `AUTH_RESPONSE`.
     - `handle_send_receipt` (Java `:515`) — correlate with `producers[producerId]`, fire `ConnectionEvent::SendReceipt`.
     - `handle_send_error` — surface as event; producer state machine may switch to fenced (PIP-68).
     - `handle_message` — peel optional `0x0e02` broker-entry-metadata (PIP-90), peel `0x0e01` envelope, decompress if needed (CompressionType from MessageMetadata), explode batch if `num_messages_in_batch > 1`, route to `consumers[consumerId]`.
     - `handle_ping` / `handle_pong` — keepalive (Java `ClientCnx.java` `:handlePing/handlePong`).
     - `handle_lookup_response` — feeds the lookup state machine (see step 7).
     - `handle_topic_migrated` — PIP-188; emit `ConnectionEvent::Reconnect { target: NewBroker }`.
     - `handle_ack_response` — PIP-54 / PIP-391; correlate with pending acks.
     - `handle_close_producer` / `handle_close_consumer` — broker-initiated close.
     - `handle_active_consumer_change` — Failover/Exclusive notification.
     - `handle_get_last_message_id_response` — PIP-296.
     - `handle_watch_topic_list_*` — PIP-145 (feeds TopicListWatcher state machine, step 9).
     - `handle_get_schema_response` / `handle_get_or_create_schema_response` — schema registry round-trips.
   - Tests: see §10 below.

3. **`Producer` state machine** at `crates/quasar-pulsar-proto/src/producer.rs`.
   - Mirrors `ProducerImpl.java:113` (research §3).
   - State:
     - `handler_state: HandlerState`,
     - `producer_id: u64`,
     - `pending_sends: VecDeque<OpSendMsg>` (mirrors Java's `pendingMessages`; `OpSendMsg` carries sequence-id, payload, metadata, optional chunk context),
     - `batch_container: Option<BatchContainer>` (mirrors `ProducerImpl.java:135`),
     - `last_seq_id_published: i64` (`:153`), `last_seq_id_pushed: i64` (`:158`),
     - `access_mode: pb::ProducerAccessMode` (PIP-68),
     - `dedup_enabled: bool`,
     - `chunked: Option<ChunkContext>` (PIP-37/107/131 — uuid, num_chunks, chunk_id).
   - Pure actions:
     - `submit(msg) -> Result<MessageId, ProducerError>`: assign sequence-id, optionally batch, optionally chunk (split into N frames each with its own `MessageMetadata.uuid + chunk_id + num_chunks_from_msg + total_chunk_msg_size` — research §2, §5 PIP-37/107/131).
     - `on_send_receipt(receipt)`: mark in-flight as published, advance `last_seq_id_published`, fire `ProducerEvent::SendOk`.
     - `on_send_error(err)`: route to `Failed` or `Fenced` per Java's `ProducerImpl.java:1570` chunked-context cleanup.
     - `poll_transmit() -> Option<Bytes>`: pop next ready frame.
     - `tick(now)`: drive batch-timeout if `batch_container.is_some()`.
   - Tests:
     - dedup ledger correctness on retry: `last_seq_id_published` does not regress.
     - chunking: a 5MB message with `max_message_size = 1MB` → 5 frames carrying same uuid, ascending chunk_id, total_chunk_msg_size set on each.
     - exclusive producer (`AccessMode::Exclusive`): broker `SEND_ERROR(ProducerFenced)` → state machine transitions to `ProducerFenced`, surfaces event.

4. **`Consumer` state machine** at `crates/quasar-pulsar-proto/src/consumer.rs`.
   - Mirrors `ConsumerImpl.java:143` (research §3).
   - State:
     - `handler_state: HandlerState`,
     - `consumer_id: u64`,
     - `subscription: SubscriptionSpec` (name, type, initial position, key_shared mode),
     - `receiver_queue: VecDeque<IncomingMessage>` (mirrors `ConsumerImpl.java:528-531`),
     - `receiver_queue_size: usize`, `available_permits: i32`,
     - `flow_threshold: i32 = receiver_queue_size / 2`,
     - `ack_tracker: AckTracker` (PIP-54/391),
     - `nack_tracker: NegativeAcksTracker`,
     - `unacked_tracker: UnAckedMessageTracker`,
     - `chunks_in_flight: HashMap<ChunkUuid, ChunkAssembly>` (PIP-37 reassembly),
     - `dead_letter_policy: Option<DeadLetterPolicy>` (PIP-22),
     - `retry_letter_policy: Option<RetryLetterPolicy>` (PIP-58),
     - `seek_status: SeekStatus`,
     - `broker_entry_metadata_enabled: bool` (PIP-90 — set from negotiated FeatureFlags).
   - Pure actions:
     - `on_message(frame)`: extract broker_entry_metadata if envelope present; if compressed, decompress; if batched, explode into individual messages; if chunked, accumulate in `chunks_in_flight` until last chunk → emit single logical message; push to `receiver_queue`; decrement `available_permits`; if below `flow_threshold` → enqueue `CommandFlow`.
     - `receive() -> Option<IncomingMessage>`: pop from `receiver_queue`, register in `unacked_tracker`.
     - `ack(ack)`: route through `ack_tracker` (groups for `ackGroupTimeMs`, supports cumulative + individual + batch-index bitset via `ack_set`).
     - `negative_ack(id)`: push to `nack_tracker` with redelivery delay.
     - `seek(target)`: emit `CommandSeek`; freeze `receiver_queue` until response (`SeekStatus::InProgress -> Done`).
     - `redelivery_for_dlq(msg)`: when message exceeds `dead_letter_policy.max_redeliver_count`, publish via the configured DLQ producer (PIP-22/PIP-124 — broker is mostly unaware; the consumer is the one routing).
     - `tick(now)`: drive each tracker, emit flow control if window opens.
   - Tests: per research §4, mirror `AcknowledgementsGroupingTrackerTest.java`, `BatchMessageContainerImplTest.java`, `MessageIdCompareToTest.java`, `ChunkMessageIdImplTest.java`, `TopicListWatcherTest.java`.

5. **Trackers** at `crates/quasar-pulsar-proto/src/trackers/`. Each is a pure tick-driven state machine.
   - `ack_tracker.rs`: groups individual + cumulative ACKs over `ack_group_time` (mirrors `PersistentAcknowledgmentsGroupingTracker.java:707-742`). `poll(now) -> Vec<pb::CommandAck>`. PIP-54 batch bitset support via `MessageIdData.ack_set`.
   - `negative_acks_tracker.rs`: time-bucketed set of (messageId → redelivery deadline). `poll(now) -> Vec<MessageId>` (mirrors `NegativeAcksTracker.java:44-216`).
   - `unacked_tracker.rs`: sliding-window expirations (mirrors `UnAckedMessageTracker.java:45-...`). `poll(now) -> Vec<MessageId>`.
   - Tests: deterministic clock drives `tick(now + N ms)` → assert which messages surface.

6. **`Backoff`** at `crates/quasar-pulsar-proto/src/backoff.rs`.
   - Direct port of `pulsar-client/src/main/java/org/apache/pulsar/client/impl/Backoff.java`. Truncated exponential + jitter.
   - Pure: `Backoff::next() -> Duration`. Engine-side, the runtime sleeps that long.
   - Tests: bound check (`min <= duration <= max`), jitter range, reset semantics.

7. **`LookupStateMachine`** at `crates/quasar-pulsar-proto/src/lookup.rs`.
   - Mirrors `BinaryProtoLookupService.java:56` (research §3).
   - Inputs: `lookup_topic(topic, authoritative) -> LookupRequest`, `lookup_partitions(topic) -> PartitionedMetadataRequest`.
   - Handles `LookupType::Redirect` recursion (`getBroker` at `BinaryProtoLookupService.java:146`).
   - PIP-344: emit `CommandPartitionedTopicMetadata.metadata_auto_creation_enabled = false` when the FeatureFlag is negotiated.
   - Tests: redirect chain (broker A → broker B → broker C), exhausting retries → emit `LookupError::TooManyRedirects`.

8. **`PulsarServiceNameResolver`** at `crates/quasar-pulsar-proto/src/url.rs`.
   - Mirrors `PulsarServiceNameResolver.java` (research §3). Parses `pulsar://host[,host,...]:port`, `pulsar+ssl://...`; round-robin rotation.
   - Pure URL parsing + rotation state.
   - Tests: comma-separated multi-host, scheme detection (`pulsar` → 6650, `pulsar+ssl` → 6651), rotation determinism.

9. **`TopicListWatcher`** at `crates/quasar-pulsar-proto/src/watcher.rs`.
   - PIP-145 (research §5; commands at proto:1229-1232).
   - Inputs: regex pattern, namespace. Outputs: `WatcherEvent::Added(topic)`, `WatcherEvent::Removed(topic)`. Internal: track `topics_known` set; on `WATCH_TOPIC_UPDATE` compute diff.
   - Mirrors Java `TopicListWatcherTest.java` (research §4).

10. **Event union** at `crates/quasar-pulsar-proto/src/event.rs`:
    - `pub enum ConnectionEvent { Connected, ProducerReady(ProducerHandle), ProducerClosed(ProducerHandle, ProducerCloseReason), Subscribed(ConsumerHandle), ConsumerClosed(ConsumerHandle, ConsumerCloseReason), Message { handle: ConsumerHandle, message: IncomingMessage }, SendReceipt { handle: ProducerHandle, message_id: MessageId, sequence_id: i64 }, SendError { handle: ProducerHandle, error: ServerError }, AckResponse { handle: ConsumerHandle, response: AckResponse }, AuthChallenge { challenge: Bytes }, Reconnect { target: BrokerAddress }, TopicMigrated { broker: BrokerAddress }, LookupResult(LookupResult), ... }`.

### Docs to update at M2

- `crates/quasar-pulsar-proto/README.md`: API surface, sans-io semantics, "Compared to Java" map of class → Rust module.
- `docs/architecture.md` (new): the sans-io diagram, request-id correlation tables, how engines drive the state machine.

### Validation gate for M2

`cargo build -p quasar-pulsar-proto --all-features && cargo clippy -p quasar-pulsar-proto --all-features -- -D warnings && cargo test -p quasar-pulsar-proto`. **Crucial: this milestone is signed off only when no `tokio`, no `async`, no `mio`, no `socket2` appears in `cargo tree -p quasar-pulsar-proto`.** (Hard constraint from research §6.)

### Risks for M2

- The sans-io split forces an explicit policy on dynamic dispatch: do we want `dyn AuthDataProvider` inside `Connection`, or `Connection<A: AuthDataProvider>`? Default: type-parameterise `Connection<A: AuthDataProvider>` for zero-cost; provide a `BoxedAuthProvider` for ergonomic non-monomorphised use.
- `chunked` message reassembly is stateful and can OOM under attack; cap `chunks_in_flight.len()` (default 100, configurable). Mirror Java's `ConsumerImpl.maxPendingChunkedMessage`.

---

## 5. Tokio engine (Milestone M3, in `quasar-pulsar-runtime-tokio`)

### Context

- Tokio is the public-default engine (research §11 Q14). Battle-tested; TLS via `tokio-rustls`; tracing-integrated.
- Engine responsibility: feed bytes into `Connection::handle_bytes`, drain bytes via `poll_transmit`, schedule timers via `poll_timeout`/`handle_timeout`, surface events via `poll_event`.

### Steps

1. **`Engine` trait** in `quasar-pulsar` (the façade, not the engine crate — engines are concrete impls).
   - `pub trait Engine: Send + 'static { type Connect: Future<Output = Result<EngineConnection, EngineError>> + Send; fn connect(&self, addr: BrokerAddress) -> Self::Connect; fn now(&self) -> Instant; fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send; }` (using native `async fn` in traits since edition 2024).

2. **`TokioEngine`** in `crates/quasar-pulsar-runtime-tokio/src/engine.rs`:
   - `impl Engine for TokioEngine`.
   - `connect(addr)`: `TcpStream::connect`, then if `pulsar+ssl://`, `TlsConnector::from(Arc<ClientConfig>).connect(domain, stream)`.
   - Rustls only — no native-tls (research §15 allow-list).

3. **Connection actor** in `crates/quasar-pulsar-runtime-tokio/src/actor.rs`:
   - One `tokio::task::spawn` per `Connection` (matches Java's one-Netty-channel-per-broker model — `ClientCnx.java`).
   - Inside: a `tokio::select!` loop:
     - `socket.read_buf(&mut in_buf)` → `connection.handle_bytes(now, &in_buf); in_buf.clear()`.
     - Outbound: pop `Connection::poll_transmit(&mut tx_buf)` → `socket.write_all(&tx_buf).await`.
     - Timer: `tokio::time::sleep_until(connection.poll_timeout())` → `connection.handle_timeout(now)`.
     - Commands from `tokio::sync::mpsc::Receiver<EngineCommand>` (open_producer, subscribe, send, ack, close, ...).
     - Events: drain `connection.poll_event()` → `tokio::sync::broadcast::Sender<ConnectionEvent>` or per-producer/-consumer mpscs.
   - On `EOF` or error → backoff (sans-io `Backoff::next()`) → `TcpStream::connect` again → resume.

4. **`PulsarClient::builder().build()`** in `quasar-pulsar/src/builder.rs`:
   - Defaults to `TokioEngine` when the `tokio` feature is enabled (the default).
   - Returns `Arc<PulsarClient>` with handles `producer(...).await`, `consumer(...).await`, `reader(...).await`.

5. **Examples** in `crates/quasar-pulsar/examples/`:
   - `examples/produce_consume.rs` (golden path).
   - `examples/key_shared.rs` (PIP-34/119/282).
   - `examples/chunking.rs` (PIP-37).
   - `examples/dead_letter.rs` (PIP-22).

### Docs to update at M3

- `crates/quasar-pulsar-runtime-tokio/README.md`: TLS setup (rustls config), tracing integration, examples.
- `docs/quickstart.md` (new): copy-pasteable producer + consumer snippet against `pulsar://localhost:6650`.

### Validation gate for M3

`cargo build -p quasar-pulsar-runtime-tokio --all-features && cargo clippy -p quasar-pulsar-runtime-tokio -- -D warnings && cargo test -p quasar-pulsar-runtime-tokio && cargo run -p quasar-pulsar --example produce_consume` (against a local docker `apachepulsar/pulsar:3.0.x`).

### Risks for M3

- Backpressure: `tokio::sync::mpsc` channel sizing — bound by `Connection.receiver_queue_size`. Cap producer-side `pending_sends` (configurable; default 1000) to match Java's `maxPendingMessages`.
- PROXY-protocol — defer to v0.2.0 (research §16). Document as known gap.

---

## 6. moonpool engine (Milestone M4, in `quasar-pulsar-runtime-moonpool`)

### Context

- moonpool is the **differentiator**: it lets users replay bug reports under `moonpool-sim` with deterministic seeds, full chaos (`buggify!`), and FoundationDB-style invariants (`assert_always!`/`assert_sometimes!`). (Research §6.)
- Crate depends on `moonpool-core` only (NOT `moonpool-transport` — its wire format is incompatible with Pulsar's. Research §6.)
- moonpool labels itself "hobby-grade" — we accept this risk because the sans-io split keeps it isolated to one crate. If moonpool's API churns, only `quasar-pulsar-runtime-moonpool` rebuilds.

### Steps

1. **`MoonpoolEngine`** in `crates/quasar-pulsar-runtime-moonpool/src/engine.rs`:
   - `pub struct MoonpoolEngine<N: NetworkProvider, T: TimeProvider, R: RandomProvider> { net: N, time: T, rng: R }`.
   - `impl<N, T, R> Engine for MoonpoolEngine<N, T, R>`.
   - `connect(addr)`: `self.net.connect(NetworkAddress::from(addr)).await`.
   - `now()`: `self.time.now()`.
   - `sleep_until(deadline)`: `self.time.sleep_until(deadline).await`.

2. **Connection actor** in `crates/quasar-pulsar-runtime-moonpool/src/actor.rs`:
   - Same shape as tokio actor but using moonpool's task abstraction (`TaskProvider`).
   - The same `Connection` sans-io state machine drives both engines — that's the entire point.

3. **moonpool TLS gap (known issue)**:
   - `moonpool-core::NetworkProvider` does not expose `tokio::io::AsyncRead`/`AsyncWrite`; rustls needs `std::io::Read`+`Write` or an `AsyncRead`+`AsyncWrite`-like abstraction (research §6).
   - **Three resolution options**:
     - **(a) Upstream PR** to `moonpool-core` to expose its connection type as `AsyncRead + AsyncWrite` (probably the right long-term answer; we are early-adopters and Pierre is open to contribution).
     - **(b) Local rustls adapter**: build a thin `Read`+`Write` shim on top of moonpool's connection bytes API, then run rustls's synchronous `ClientConnection::{read_tls, write_tls, process_new_packets}` over it. rustls is itself sans-io, so this is sound.
     - **(c) Ship v0.1.0 moonpool engine TLS-less.** Document the gap. Recommend tokio engine for users needing TLS.
   - **Default proposal for v0.1.0**: (c) — TLS-less moonpool engine; raise upstream issue for (a); revisit (b) for v0.2.0 if upstream rejects. Tokio engine continues to ship TLS.

4. **Deterministic sim example** in `crates/quasar-pulsar-runtime-moonpool/examples/sim_replay.rs`:
   - Build the moonpool sim providers (`SimProviders::new(seed)`); construct `MoonpoolEngine` over them; drive a producer/consumer scenario; on failure, the seed reproduces deterministically. Mirrors moonpool's own examples (research §6).

5. **Sim-driven test harness** in `crates/quasar-pulsar-runtime-moonpool/tests/sim/`:
   - `sim::network_disconnect_during_subscribe`: chaos profile drops the TCP connection between SUBSCRIBE send and SUCCESS reply; assert the consumer state machine reconnects with backoff and resumes.
   - `sim::partial_write_during_send`: chaos profile delivers only half the bytes of a SEND frame; broker times out; consumer redelivery fires; client republishes.
   - `sim::bit_flip_payload`: chaos profile flips a byte in payload; CRC32C mismatch surfaces as `ProtoError::Framing(FrameError::ChecksumMismatch)`.
   - Invariants: `assert_always!(consumer.unacked_count() <= consumer.receiver_queue_size * 2)`.
   - These tests run under `moonpool-sim` and reproduce deterministically from a seed.

### Docs to update at M4

- `crates/quasar-pulsar-runtime-moonpool/README.md`: moonpool intro, "this engine is for deterministic-sim testing; for production, prefer the tokio engine", TLS gap status, link to sim example.
- `docs/simulation-testing.md` (new): how to run quasar under `moonpool-sim`, seed reproduction, chaos profiles.

### Validation gate for M4

`cargo build -p quasar-pulsar-runtime-moonpool --all-features && cargo clippy -p quasar-pulsar-runtime-moonpool -- -D warnings && cargo test -p quasar-pulsar-runtime-moonpool`.

### Risks for M4

- moonpool v0.6.x → v0.7 may break our integration. Pin minor: `moonpool-core = "=0.6"` until we cut v0.2.0.
- moonpool's `async-trait` use (research §6) bleeds into our APIs unless we re-wrap. Default: keep our public traits using native `async fn` in traits (edition 2024); use `BoxFuture`-typed wrappers inside `quasar-pulsar-runtime-moonpool` only.

---

## 7. Schema layer (Milestone M5, in `quasar-pulsar`)

### Context

- Java client supports an exhaustive schema matrix (research §3): 17 primitive + Avro + JSON + Protobuf + ProtobufNative + KeyValue + AutoConsume + AutoProduce + generic.
- v0.1.0 scope per research §11 Q6 (default proposal (a)): bytes + String + Json + raw Avro + Protobuf. Defer KeyValue / AutoConsume / AutoProduce / ProtobufNative / full generic to v0.2.0.

### Steps

1. **`Schema` trait** at `crates/quasar-pulsar/src/schema/mod.rs`:
   - `pub trait Schema { type Item; fn encode(item: &Self::Item) -> Bytes; fn decode(bytes: &[u8]) -> Result<Self::Item, SchemaError>; fn schema_info(&self) -> SchemaInfo; }`.
   - `SchemaInfo` carries name + type (`pb::Schema_Type`) + bytes (Avro JSON schema, Protobuf descriptor, etc.).

2. **Built-in schemas**:
   - `BytesSchema` (no-op).
   - `StringSchema` (UTF-8).
   - `JsonSchema<T: Serialize + DeserializeOwned>` via `serde_json` (allow-list).
   - `AvroSchema<T: serde::Serialize + serde::Deserialize>` via `apache-avro` (allow-list).
   - `ProtobufSchema<T: prost::Message + Default>` via `prost` (allow-list).

3. **Schema registry**:
   - Pure code in `quasar-pulsar-proto::Connection` already handles `GET_OR_CREATE_SCHEMA` / `GET_SCHEMA` correlation (M2).
   - Façade in `quasar-pulsar/src/schema/registry.rs` wraps the request as a future, caches `(topic, schema_version) -> SchemaInfo`.
   - Tests: round-trip against the broker fake.

4. **Out-of-scope for v0.1.0** (stub with `unimplemented!()` + behind `schema-extras` feature flag):
   - `KeyValueSchema`, `AutoConsumeSchema`, `AutoProduceBytesSchema`, `ProtobufNativeSchema`, `GenericRecord`.
   - Document in `crates/quasar-pulsar/README.md` and `docs/schema-support.md` with a clear "supported / not yet" matrix.

### Validation gate for M5

`cargo build -p quasar-pulsar --features=tokio && cargo test -p quasar-pulsar --features=tokio`.

---

## 8. Auth layer (Milestone M6, in `quasar-pulsar`)

### Context

- Java client has six auth providers (research §3): disabled, basic, token, TLS, OAuth2, SASL, Athenz, KeyStoreTls.
- v0.1.0 scope per research §11 Q10 (default proposal (a)): token + TLS (mTLS) + the **AUTH_CHALLENGE refresh hook** (PIP-30 — research §5). Defer OAuth2 / SASL / Athenz.

### Steps

1. **`Authentication` trait** at `crates/quasar-pulsar/src/auth/mod.rs`:
   - `pub trait Authentication: Send + Sync { fn name(&self) -> &str; fn data(&self) -> AuthData; fn on_challenge(&self, challenge: &[u8]) -> Result<AuthData, AuthError>; }`.
   - `AuthData = pb::AuthData { auth_method_name, auth_data }`. Matches Java's `AuthenticationDataProvider`.

2. **Providers**:
   - `AuthenticationDisabled` (no-op).
   - `AuthenticationToken { token: Cow<'static, str> | TokenSource }` — `name = "token"`, `data = token.as_bytes()`. Mirrors `AuthenticationToken.java`.
   - `AuthenticationTls { cert: Vec<u8>, key: Vec<u8> }` — mTLS handshake handled by `rustls`. Mirrors `AuthenticationTls.java`.

3. **AUTH_CHALLENGE refresh** (PIP-30 / PIP-292):
   - The `Connection::handle_auth_challenge` already routes to the configured provider's `on_challenge` (M2 step 2). The provider returns fresh credentials; `Connection` enqueues `CommandAuthResponse`. Pure sans-io.

4. **Deferred** (gate behind feature flags; stubbed with `unimplemented!()`):
   - `auth-oauth2` feature → `AuthenticationOAuth2` (mirrors `oauth2/AuthenticationOAuth2.java`, `ClientCredentialsFlow.java`). v0.2.0.
   - `quasar-pulsar-auth-sasl` (separate crate, parallel to Java's `pulsar-client-auth-sasl`). v0.3.0+.
   - `quasar-pulsar-auth-athenz` (separate crate). v0.3.0+.

### Validation gate for M6

`cargo build -p quasar-pulsar --features=tokio,auth && cargo test -p quasar-pulsar --features=tokio,auth && cargo run -p quasar-pulsar --example token_auth` (against a broker with `authenticationEnabled=true`).

---

## 9. Admin REST client (post-v0.1.0)

### Context

- Java's `pulsar-client-admin/` (research §3) is a JAX-RS / Jersey client over a separate REST endpoint (default port 8080).
- Separate crate `quasar-pulsar-admin`, depends on `reqwest` (allow-list) + `serde`.
- Out of scope for v0.1.0 release; scaffolded only.

### Steps (deferred)

1. Crate skeleton with `pub struct AdminClient` and one method (`async fn cluster_info()`).
2. JSON DTOs for `Namespaces`, `Topics`, `Tenants` etc. — mirror `pulsar-client-admin-api` (Java).
3. Per-resource modules (`namespaces.rs`, `topics.rs`, `tenants.rs`, `clusters.rs`, ...).
4. v0.2.0 ship plan: separate `quasar-pulsar-admin v0.1.0` release after `quasar-pulsar v0.1.0`.

---

## 10. Test strategy

### Layers (in increasing cost order)

| Layer | Location | Runtime needed | Speed | Purpose |
|---|---|---|---|---|
| **Unit (sans-io)** | `crates/quasar-pulsar-proto/tests/` | none | μs–ms | State-machine, codec, tracker correctness |
| **Broker fake** | `crates/quasar-pulsar-proto/tests/with_fake.rs` via `quasar-pulsar-fakes` | none | ms | Per-command fault injection (mirror `MockBrokerService.java`) |
| **Sim chaos** | `crates/quasar-pulsar-runtime-moonpool/tests/sim/` | moonpool-sim | s | Network chaos, deterministic replay |
| **E2e** | `xtask/e2e/` (or `crates/quasar-pulsar/tests/e2e.rs` gated on `e2e` feature) | tokio + docker | 10s+ | Real broker behavior, Pulsar 3.0.x |
| **Fuzz** (post-v0.1.0) | `crates/quasar-pulsar-proto/fuzz/` | none | continuous | `frame::decode` invariants |

### 10 representative unit tests to write (mirror Java, research §4)

1. `crates/quasar-pulsar-proto/tests/connection_handshake.rs` — mirrors `ClientCnxTest.java` (`:254-280`): drive `Connection` through `CONNECT → CONNECTED`; assert state transitions, FeatureFlags negotiation.
2. `crates/quasar-pulsar-proto/tests/ack_tracker.rs` — mirrors `AcknowledgementsGroupingTrackerTest.java`: tick `AckTracker::poll(now + N ms)`, assert grouped ACK frames.
3. `crates/quasar-pulsar-proto/tests/batch_container.rs` — mirrors `BatchMessageContainerImplTest.java`: pack 100 messages with batching policy, assert produced frame matches Java's encoding byte-for-byte (capture via a vendored Java byte vector).
4. `crates/quasar-pulsar-proto/tests/lookup.rs` — mirrors `BinaryProtoLookupServiceTest.java`: simulate `Redirect → Connect → Redirect → Connect → Final`; assert final broker selected.
5. `crates/quasar-pulsar-proto/tests/message_id_cmp.rs` — mirrors `MessageIdCompareToTest.java`: proptest over all `(ledger_id, entry_id, partition, batch_index)` combinations; assert ordering.
6. `crates/quasar-pulsar-proto/tests/chunk_message_id.rs` — mirrors `ChunkMessageIdImplTest.java`: serialize/deserialize ChunkMessageId with `first_chunk_message_id` set (PIP-107).
7. `crates/quasar-pulsar-proto/tests/topic_list_watcher.rs` — mirrors `TopicListWatcherTest.java` (PIP-145).
8. `crates/quasar-pulsar-proto/tests/fault_injection.rs` — mirrors `ClientErrorsTest.java` via `quasar-pulsar-fakes::FakeBroker`. Per `MockBrokerService.java` hooks: simulate `CONNECT` returning `ServerError::ServiceNotReady`; assert client backs off and retries.
9. `crates/quasar-pulsar-proto/tests/chunking_roundtrip.rs` — PIP-37/107/131: producer chunks 5MB message, consumer reassembles.
10. `crates/quasar-pulsar-proto/tests/key_shared_dispatch.rs` — PIP-34/119/282/379: subscribe with `KeySharedMode::AutoSplit`, simulate broker's hash-based dispatch; assert consumer state matches Java client behavior.

### Broker fake (`quasar-pulsar-fakes`)

- API:
  ```text
  pub struct FakeBroker { /* ... */ }
  impl FakeBroker {
      pub fn new() -> Self;
      pub fn on_connect(self, hook: impl Fn(&pb::CommandConnect) -> pb::CommandConnected + Send + 'static) -> Self;
      pub fn on_subscribe(self, hook: impl Fn(&pb::CommandSubscribe) -> Result<pb::CommandSuccess, pb::ServerError> + Send + 'static) -> Self;
      pub fn on_send(self, hook: ...) -> Self;
      pub fn on_flow(self, hook: ...) -> Self;
      pub fn on_ack(self, hook: ...) -> Self;
      pub fn handle(&mut self, frame: Frame) -> Vec<Frame>; // sync, in/out frames
  }
  ```
- Mirrors `MockBrokerService.java` / `MockBrokerServiceHooks.java` (research §4).
- Pure sans-io. No sockets. Drives the same `frame::encode`/`decode` API as the client.

### E2e (`xtask/e2e/`)

- Tooling: `testcontainers-rs` (allow-list) + `apachepulsar/pulsar:3.0.x` standalone container (research §4: `StandaloneContainer.java:28-41`, `PulsarContainer.java:66`).
- Tests (each `#[tokio::test]` gated on `e2e` feature):
  - `e2e_produce_consume_golden_path`.
  - `e2e_partitioned_topic` (3 partitions, key-based routing).
  - `e2e_key_shared_dispatch` (PIP-34).
  - `e2e_chunking_5mb` (PIP-37, with `maxMessageSize` override).
  - `e2e_batch_index_ack` (PIP-54/391).
  - `e2e_dead_letter` (PIP-22/124).
  - `e2e_retry_letter` (PIP-58/409).
  - `e2e_topic_migrated` (PIP-188 — drive a cluster failover scenario).
  - `e2e_topic_list_watcher` (PIP-145 — subscribe by regex, create new matching topic).
  - `e2e_auth_challenge` (PIP-30 — token refresh in flight).
- Fallback: `docker-compose.yml` under `xtask/e2e/compose/` for local dev when testcontainers's daemon detection is flaky.

### Fuzz (post-v0.1.0)

- `cargo-fuzz` harness for `frame::decode(&mut BytesMut)` invariants:
  - Never panics.
  - Never reports `Ok(Some(...))` for a frame shorter than its declared `TOTAL_SIZE`.
  - Idempotent: replaying the same input on a fresh `BytesMut` yields the same `Frame`.
- Schedule: post-v0.1.0; add as a tracked Issue but not a release blocker.

---

## 11. Documentation

Quasar docs ship in three buckets: rustdoc on every public item, repo-root `*.md` files, and `docs/` deep dives. **All behavior changes update docs in the same changeset** (CLAUDE.md "docs are code").

### Repo-root files

- `README.md` — quickstart + status banner + supported PIPs link.
- `LICENSE` — Apache-2.0 boilerplate.
- `NOTICE` — copyright + Clever Cloud attribution.
- `GUIDELINES.md` — see below.
- `AGENTS.md` — agent rules (M0 step 14).
- `CHANGELOG.md` — release notes (Keep-a-Changelog format).
- `CONTRIBUTING.md` — branch naming, commit conventions, validation chain, no-Claude-trailer rule.

### `GUIDELINES.md` content (full text to write at M0)

- **Protocol correctness invariants** (mirror research §10):
  - All inbound bytes pass through `frame::decode` which verifies `0x0e01` magic + CRC32C; mismatches surface as errors, never panics.
  - Broker-entry-metadata envelope (`0x0e02`) is detected before standard frame decoding (PIP-90).
  - `Connection.next_request_id` is strictly monotonic; correlation table never reuses a live id.
  - `Connection.next_sequence_id` per producer is strictly monotonic; dedup `last_seq_id_published` never regresses.
  - Chunked-message reassembly is bounded (`max_chunks_in_flight`, default 100); over-quota → drop oldest with logged error.
  - `quasar-pulsar-proto` has zero panics outside of explicit invariant violations; all errors flow through `ProtoError`.
- **Code style** (mirror user's CLAUDE.md):
  - `ToOwned::to_owned()` over `Clone::clone()` for ownership intent clarity.
  - `bytes::Bytes` and `&[u8]` for byte payloads; never `Vec<u8>` in public APIs.
  - No `Arc<Mutex<...>>` in `quasar-pulsar-proto` (sans-io, single-threaded by design).
  - No `unsafe` outside of `prost`-generated code and documented FFI shims.
- **Worktree-first** (mirror CLAUDE.md): all post-bootstrap work via `wt switch --create <branch> -y`. Conventional commits, `-s -S` signed. No `Generated by Claude` trailers.
- **Validation chain**: `cargo build --all-features && cargo clippy --all-features -- -D warnings && cargo +nightly fmt && cargo test --workspace` before declaring any task complete.
- **PIP support matrix**: full table mirroring research §5, with v0.1.0 / v0.2.0+ columns.

### `docs/` deep dives

- `docs/architecture.md` — sans-io + engines diagram (created at M2).
- `docs/protocol.md` — wire-format reference (created at M1).
- `docs/quickstart.md` — first producer/consumer (created at M3).
- `docs/schema-support.md` — supported / not-yet matrix (created at M5).
- `docs/simulation-testing.md` — moonpool-sim usage (created at M4).
- `docs/migration-from-pulsar-rs.md` — for Florentin's existing users; created at v0.1.0 cut.

### Rustdoc obligations

- Every public item carries `///` with usage example. CI runs `cargo doc --no-deps --all-features` and fails on `missing_docs` once we set `#![deny(missing_docs)]` per public crate (target M2 for `quasar-pulsar-proto`, M3 for `quasar-pulsar`).

---

## 12. CI / release engineering

### GitHub Actions matrix

- **`fmt-check`** (nightly): `cargo +nightly fmt --check`. Fast fail.
- **`clippy`** (stable, all features): `cargo clippy --workspace --all-features -- -D warnings`.
- **`build`** (stable, all features): `cargo build --workspace --all-features`.
- **`test`** (stable, all features): `cargo test --workspace --all-features` (excluding `e2e`).
- **`doc`** (stable): `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps` — `cargo doc` does not accept `-D warnings` directly (reviewer C-7).
- **`deny`** (stable): `cargo deny check` (advisories + bans + licenses + sources); covers `cargo audit` (reviewer C-6).
- **`e2e`** (stable, `workflow_dispatch` + release branches): `cargo test --workspace --features e2e` with Docker available.
- Caching via `Swatinem/rust-cache@v2`.

### Release engineering

- **Versioning**: `cargo-release` (post-v0.1.0). Tag `vX.Y.Z`; per-crate version bumps in lockstep until APIs stabilise (`workspace.package.version`).
- **Publishing**: each crate published in dep-order: `quasar-pulsar-proto`, then `quasar-pulsar-runtime-tokio` and `quasar-pulsar-runtime-moonpool` in parallel, then `quasar-pulsar`, then `quasar-pulsar-admin` (when ready). Approval-gated (research §15).
- **License headers**: Apache-2.0 SPDX boilerplate `// SPDX-License-Identifier: Apache-2.0` on every `.rs` file (enforced by `xtask license-check`).
- **`cargo deny`**: enforces allow-listed licenses (Apache-2.0, MIT, BSD-3-Clause, ISC), denies advisories.
- **`cargo-vet` or `cargo-audit`** — pick at v0.2.0; defer.

---

## 13. Milestone schedule + ordering

```
M0 (bootstrap, 1-2 days)
  └─> M1 (codec, ~1 week)
        └─> M2 (sans-io state machines, ~3-4 weeks)
              ├─> M3 (tokio engine, ~1-2 weeks)        ─┐
              ├─> M4 (moonpool engine, ~1-2 weeks)     ─┼─> M5 (schemas, ~3 days) ─> M6 (auth, ~3 days) ─> v0.1.0 cut
              └─> M5 (schemas, can start mid-M2)       ─┘
```

- **M0** must finish before any other milestone (CI gates everything).
- **M1** strictly precedes M2 (state machines need codec).
- **M2** is the long pole. Sub-milestones inside M2 can be parallelised:
  - M2a `Connection` + handshake.
  - M2b `Producer` + dedup + chunking.
  - M2c `Consumer` + trackers + flow control.
  - M2d Lookup + Watcher + URL resolver.
- **M3 (tokio) and M4 (moonpool)** can run in parallel once M2 hits 80% — both consume the same `Connection` API. Prioritise M3 (public default).
- **M5 schemas** has only soft dependency on M2: registry round-trips need `Connection`, but pure schema codecs (Bytes / String / Json / Avro / Protobuf) can be written against the codec from M1.
- **M6 auth** depends on M2 (AUTH_CHALLENGE wiring) and M3 (mTLS via tokio-rustls).

### Suggested calendar (rough)

| Week | Milestones |
|---|---|
| 1 | M0 + start M1 |
| 2 | M1 finish, start M2a |
| 3–6 | M2 (parallelised M2a–M2d) |
| 5–7 | M3 (start mid-M2) |
| 5–7 | M4 (start mid-M2, in parallel with M3) |
| 6 | M5 (slot when M1 lands) |
| 7 | M6 |
| 8 | v0.1.0 release prep — docs, examples, CHANGELOG, publish to crates.io (approval-gated) |

---

## 14. Validation

Per `~/.claude/CLAUDE.md`, the validation chain at the end of every milestone is:

```
cargo build --workspace --all-features
cargo clippy --workspace --all-features -- -D warnings
cargo +nightly fmt --check
cargo test --workspace
```

E2e tests are gated:

```
cargo test --workspace --features e2e
```

Run on demand (requires Docker). CI runs e2e on `workflow_dispatch` and release branches.

Per-milestone gates are documented inline in §2–§8.

---

## 15. Approval-gated actions

The implementer **must NOT** do any of the following without explicit user OK:

1. **Choosing the published crate name.** Default proposal: `quasar-pulsar` (research §11 Q1). Alternatives: `quasar-client`, `pulsar-quasar`, `apache-pulsar-quasar`, or a new project name (`nebula`, `magnetar`, `corvus`).
2. **Choosing the license.** Default proposal: `Apache-2.0` only (matches Pulsar upstream + moonpool — research §6, §11 Q2). Alternative: MIT/Apache-2.0 dual (Rust ecosystem default + pulsar-rs).
3. **Engine set for v0.1.0.** Default proposal: `quasar-pulsar-runtime-tokio` (public default, ships TLS via tokio-rustls) + `quasar-pulsar-runtime-moonpool` (opt-in, for deterministic sim) (research §11 Q3, Q14).
4. **Crate split granularity.** Default proposal: `quasar-pulsar-proto` + `quasar-pulsar` (façade) + `quasar-pulsar-runtime-{tokio,moonpool}` + `quasar-pulsar-admin` + `quasar-pulsar-fakes` (research §11 Q4).
5. **Minimum supported broker version.** Default proposal: Pulsar 3.0 LTS (research §11 Q5; matches Apache's `PulsarContainer.java:66`).
6. **Schema scope for v0.1.0.** Default proposal: (a) bytes + String + Json + raw Avro + Protobuf, no full registry-driven KeyValue/AutoConsume/AutoProduce; deferred to v0.2.0 (research §11 Q6).
7. **Transactions in v0.1.0.** Default proposal: deferred to v0.2.0 (research §11 Q7).
8. **Admin REST client in v0.1.0.** Default proposal: scaffolded crate, unpublished; first release in v0.2.0 (research §11 Q8).
9. **CLI binary.** Default proposal: library-only for v0.1.0; CLI deferred to v0.2.0 (research §11 Q9).
10. **Auth scope for v0.1.0.** Default proposal: (a) token + TLS (mTLS) + AUTH_CHALLENGE refresh hook; OAuth2 → v0.2.0; SASL/Athenz → v0.3.0+ (research §11 Q10).
11. **Encryption (PIP-4) in v0.1.0.** Default proposal: deferred (research §11 Q11).
12. **E2e CI broker provisioning.** Default proposal: `testcontainers-rs` primary + `docker-compose.yml` fallback (research §11 Q12).
13. **Repo hosting.** Default proposal: `github.com/me/quasar` (personal). Alternatives: `github.com/CleverCloud/quasar` or a Clever Cloud OSS org (research §11 Q13).
14. **moonpool risk acceptance.** Default proposal: tokio is the public-facing default; moonpool is opt-in for deterministic sim. Documented as "hobby-grade" (research §11 Q14, §6).
15. **Coexistence with pulsar-rs.** Default proposal: ship as a separate project; revisit upstream/replacement after v0.1.0 (research §11 Q15, §8 — Florentin is a pulsar-rs maintainer).
16. **Push to GitHub.** Empty repo on `main` with zero commits; the M0 commit must be **explicitly** approved before push.
17. **Creating the GitHub repository.** If `github.com/me/quasar` does not exist yet, `gh repo create` is approval-gated.
18. **Publishing any crate to crates.io.** All eight crates require per-publish approval (researched names taken / available + permission to register a fresh name).
19. **Adding any non-trivial dependency outside the dossier's allow-list** (research §15, expanded per reviewer C-8): `prost`, `prost-build`, `prost-types`, `bytes`, `tokio`, `tokio-util`, `tokio-rustls`, `rustls`, `crc32c`, `lz4_flex`, `zstd`, `snap`, `flate2`, `serde`, `serde_json`, `apache-avro`, `tracing`, `tracing-subscriber`, `thiserror`, `anyhow`, `futures`, `futures-util`, `pin-project-lite`, `clap` (CLI only), `moonpool-core`, `testcontainers`, `rstest`, `proptest`. Any new dependency is approval-gated.
20. **Merging `wt` worktrees to `main`** (per CLAUDE.md): always confirm before `wt merge -y`.

---

## 16. Risks + mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| moonpool API churn (v0.6 → v0.7 break) | medium | low (sans-io split insulates) | Pin `moonpool-core = "=0.6"`; only `quasar-pulsar-runtime-moonpool` breaks; tokio engine unaffected. |
| moonpool TLS gap | high | medium | Ship moonpool engine TLS-less for v0.1.0; raise upstream PR (research §6, M4 step 3). Tokio engine still has TLS. |
| Pulsar protocol drift (PIP-460/466 land mid-v0.1.0) | low | low | Claim `ProtocolVersion::V21` (research §2 / proto:254); ignore unknown commands gracefully. |
| Crate-name `quasar` conflict | certain | low | Rename pre-v0.1.0 — default `quasar-pulsar` (research §9, §11 Q1). |
| pulsar-rs co-existence backlash | medium | low | Ship as a *new* project, separate repo; document scope in `docs/migration-from-pulsar-rs.md`; do not poach pulsar-rs users until parity is real (research §8, §11 Q15). |
| CRC32C perf | low | low | Use `crc32c` crate (SSE4.2 `_mm_crc32_u64` on x86_64). Benchmark on M1 close. |
| Schema parity gaps vs Java | medium | medium | Ship v0.1.0 with a clear "supported" matrix in `docs/schema-support.md` + `GUIDELINES.md`. |
| Chunked-message OOM under attack | low | high | Cap `chunks_in_flight.len()` (default 100). Logged drop on overflow. |
| AUTH_CHALLENGE token-refresh race | medium | medium | Sans-io state guarantees in-flight requests pause until `AUTH_RESPONSE` is acked (mirror `ClientCnx.java:464`). |
| `prost-build` adding a `protoc` build-time requirement | high | low | Check generated code into git via `xtask codegen`; CI re-runs and diffs (M1 step 2). |

---

## 17. Open questions for the user (prioritised)

> Each question carries the planner's default proposal. The user can answer one-by-one or in bulk.

**Tier 1 — blocks M0:**

1. **Published crate name** (research §11 Q1). Default: `quasar-pulsar`. Confirm or pick from `{quasar-client, pulsar-quasar, apache-pulsar-quasar, nebula, magnetar, corvus, ...}`.
2. **License** (research §11 Q2). Default: `Apache-2.0` only. Or MIT/Apache-2.0 dual?
3. **Engine set for day 1** (research §11 Q3). Default: tokio + moonpool, with tokio as the public default. Confirm.
4. **Crate split granularity** (research §11 Q4). Default: `quasar-pulsar-proto` + `quasar-pulsar` + `quasar-pulsar-runtime-{tokio,moonpool}` + `quasar-pulsar-admin` + `quasar-pulsar-fakes`. Or single-crate-with-features?
5. **moonpool risk acceptance** (research §11 Q14). Default: tokio is the public default; moonpool is opt-in.
6. **Repo hosting** (research §11 Q13). Default: `github.com/me/quasar`. Or `github.com/CleverCloud/quasar` / Clever Cloud OSS org?

**Tier 2 — blocks M2 scope freeze:**

7. **Minimum supported broker version** (research §11 Q5). Default: Pulsar 3.0 LTS.
8. **Schema scope for v0.1.0** (research §11 Q6). Default: (a) bytes + String + Json + raw Avro/Protobuf, no registry-driven KeyValue/AutoConsume/AutoProduce until v0.2.0.
9. **Transactions in v0.1.0** (research §11 Q7). Default: deferred to v0.2.0.
10. **Auth scope for v0.1.0** (research §11 Q10). Default: token + TLS (mTLS) + AUTH_CHALLENGE refresh; OAuth2 → v0.2.0; SASL/Athenz → v0.3.0+.
11. **Encryption (PIP-4) in v0.1.0** (research §11 Q11). Default: deferred.

**Tier 3 — blocks v0.1.0 cut:**

12. **Admin REST client in v0.1.0** (research §11 Q8). Default: scaffolded, unpublished; first release in v0.2.0.
13. **CLI binary** (research §11 Q9). Default: library-only; CLI deferred to v0.2.0.
14. **E2e CI broker provisioning** (research §11 Q12). Default: `testcontainers-rs` + `docker-compose.yml` fallback.
15. **Coexistence with pulsar-rs** (research §11 Q15). Default: separate project; revisit upstream/replacement after v0.1.0.

---

## Final checklist (before declaring v0.1.0)

- [ ] All approval-gated questions in §15/§17 answered.
- [ ] M0–M6 milestone gates passed.
- [ ] PIP support matrix in `GUIDELINES.md` matches `docs/schema-support.md` and the actual codebase.
- [ ] `cargo build --workspace --all-features` clean.
- [ ] `cargo clippy --workspace --all-features -- -D warnings` clean.
- [ ] `cargo +nightly fmt --check` clean.
- [ ] `cargo test --workspace` green.
- [ ] `cargo test --workspace --features e2e` green against `apachepulsar/pulsar:3.0.x`.
- [ ] `cargo doc --workspace --all-features --no-deps` builds cleanly (no `missing_docs` warnings on public crates).
- [ ] `cargo deny check` clean.
- [ ] `CHANGELOG.md` populated for v0.1.0.
- [ ] `docs/migration-from-pulsar-rs.md` published.
- [ ] All commits signed (`-s -S`) and conventional.
- [ ] No `Generated by Claude` / `Co-Authored-By: Claude` trailers anywhere.
- [ ] User OK to push, publish, and tag.
