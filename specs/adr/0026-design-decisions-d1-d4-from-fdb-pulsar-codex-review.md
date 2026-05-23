# ADR-0026 — D1–D4 design decisions from FoundationDB / Pulsar / Codex review

- **Status**: Accepted
- **Date**: 2026-05-23
- **Decider**: Florentin Dubois
- **Tags**: engine, façade, simulation, moonpool-sim, auth, vendored-proto, multi-source-synthesis

## Context

[`docs/follow-ups.md`](../../docs/follow-ups.md) carried four pending
design decisions (D1–D4) blocking implementation work. Florentin
authorised a multi-source synthesis: dig FoundationDB simulator design
rationale, foundationdb-rs / moonpool-sim 0.6 / TigerBeetle VOPR,
Apache Pulsar Java client architecture, and an independent Codex
review; compare the outputs; lock the decisions in an ADR.

Source agents (transcripts archived under `/tmp/claude-*/tasks/`):

- **FDB sim** (adb4ee34) — Apple's `Sim2` virtual-network model,
  event-time fast-forward, BUGGIFY, swizzle-clog. The same client
  code runs in production and in the sim; the sim replaces only the
  `INetwork` / `IAsyncFile` entry points.
- **Pulsar Java client** (a8be4aca) — `PulsarClientImpl` is shared
  infrastructure: `EventLoopGroup` + `Timer` + `ConnectionPool`.
  Surfaces (`ProducerImpl<T>`, `ConsumerImpl<T>`, `Reader<T>`,
  `TableView<T>`, `Transaction`) are **concrete generic classes**
  consuming that infrastructure. **`ClientCnx` is concrete, not an
  interface.** PIP-466 V5 is additive and does not introduce
  per-surface engine abstractions.
- **Codex** (`codex exec`, gpt-5-codex unavailable on ChatGPT auth,
  default model used) — recommended Option 2 for D1 *with* one
  façade migration in the same branch as evidence the GAT shape is
  usable; pure-sim chaos for D2; defer SASL/Athenz scope; implement
  `xtask vendor-proto` immediately.

## Decision

### D1 — Engine trait extension phase 2: **Option 1 + concrete generic surfaces**

The Pulsar-Java-client agent surfaced a fact that overturned the
follow-ups recommendation of Option 2:

> "Option 3 would mirror a *hypothetical interface-segregated* Java
> client, not the actual one. Java's `ClientImpl` shape is closer
> to Option 1: shared infrastructure (`EventLoopGroup`, `Timer`,
> `ConnectionPool`), concrete generic surfaces (`ProducerImpl<T>`,
> `ConsumerImpl<T>`, `Reader<T>`, `TableView<T>`, `Transaction`)."

Java's surfaces are not abstracted behind an interface per surface;
they are concrete classes parametrised over the schema `T` and
consuming shared executor / timer / connection state from the
client. ADR-0025 phase 1 already gave magnetar the executor + timer
half of that pattern (`Engine::spawn`, `Engine::new_interval`,
etc.).

**Lift each façade surface as a concrete generic type
`magnetar::<Surface>::<T, E: Engine>`** holding
`PulsarClient<E>::client_state()` (or an `Arc<E::ClientState>`).
No new `Engine::Producer<T>` / `Engine::Consumer<T>` GATs.

This is a refinement of the three-option menu — call it **Option 1+**
or **Option 2-light**. The trait stays small (only the ADR-0025 phase
1 primitives plus `ClientState`); the surfaces become engine-generic
via the `E` parameter, not via an `E::Producer<T>` projection.

**Cost.** Each surface lift writes its own
`<crate::magnetar>::Surface<T, E>` next to (or replacing) the
existing tokio-only façade type. Estimated 200–600 LOC per surface
plus mirrored tests per ADR-0024.

**What it forecloses.** Per-surface GAT customisation (Option 3).
If a future engine genuinely needs to override `Reader` behaviour
beyond the shared `ClientState` shape, we revisit with an
ADR-0026 amendment. None are in scope today.

**Pushback absorbed.** Codex argued Option 2 with a forced one-
surface migration. The Java-client research showed that even the
GAT itself is unnecessary if the surface types take `E` directly.
Codex's "one façade migration proves the bounds" still applies —
the first surface lift (Transaction) ships in the next commit,
not as part of this ADR.

### D2 — Wire moonpool-sim: **Option 1 (pure-sim chaos suite)** — converged

All three sources align. FDB rationale: "the simulator pays off
when the whole workload, time, network, and fault model live
inside it"; pulling in real TCP defeats determinism. Moonpool-sim's
`SimulationBuilder` exposes the exact `INetwork`-equivalent
abstraction in Rust (see the `Providers` model in
[`crates/magnetar-runtime-moonpool/src/lib.rs`](../../crates/magnetar-runtime-moonpool/src/lib.rs)).

**Land a new test target
`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`** that uses
`SimulationBuilder` + `MoonpoolEngine<SimProviders>` + an
in-simulator broker stub. The differential harness is **not**
restructured (option 2 from D2) and the broker is **not**
virtualised (option 3); both are explicitly out of scope for
v0.1.0.

**Invariant-based assertions, not byte-identical replays.** Codex
flagged this: "deterministic replay" doesn't mean "identical
EventStream every seed" — it means "identical execution for a
given seed and (across seeds) no invariant violation." First
invariants to land: at-least-once publish, ack-after-receive,
no-dup-on-acked, supervisor-recovers-within-N-ticks.

**ADR-0024 four-layer test parity is exempted** for this fixture
by construction: a moonpool-only chaos test cannot have a tokio
equivalent. The exemption goes in the commit message per ADR-0024
§Exemptions.

### D3 — SASL/Athenz: **defer full impl to v0.2.0; amend parity docs**

Codex pushed back on "ship stubs and claim full parity": that
violates ADR-0010's "full Java parity on tokio" literally. The
right move is to **defer** (the user-facing position) and
**amend** ADR-0010's parity matrix to mark SASL/Kerberos and
Athenz/ZTS as `🟡 partial — PLAIN / pre-fetched token only,
GSSAPI + ZTS in v0.2.0`. Honest parity beats stubs-as-✅.

`docs/parity-status.md` already marks both crates `🟡 pre-alpha`
([line 58–59](../../docs/parity-status.md)) but README's parity
matrix should mirror that and ADR-0010 should reference this
ADR for the scope amendment.

### D4 — Vendored proto bump: **implement `xtask vendor-proto`; milestone-based bumps**

Codex agreed with the recommendation but added an important
constraint: **do not adopt rolling "latest master" bumps**.
PIP-466 (V5 surface) is additive and the V5 client lives beside
v4; chasing proto churn before magnetar actually implements
PIP-460 / PIP-180 / PIP-33 wire pieces is friction without
benefit.

**Implement `cargo run -p xtask -- vendor-proto --rev <sha>`** to
fetch `apache/pulsar` at that commit, copy
`PulsarApi.proto` + `PulsarMarkers.proto`, update
`proto/SOURCE`, run codegen, and fail on dirty drift. Future
proto bumps reference a specific milestone or PIP in the commit
message; no automatic refresh.

## Consequences

**Easier:**

- Façade lift train (8 surfaces) becomes a sequence of concrete
  `Surface<T, E>` introductions. Each is a focused commit that
  doesn't drag the engine trait along. ADR-0025 phase 1's
  primitives are the only engine-trait-level dep.
- Pure-sim chaos lands without touching the differential harness;
  the differential train (golden traces) and the simulator train
  (invariants) become orthogonal.
- Parity docs become honest. ADR-0010's literal reading no longer
  conflicts with reality.

**Harder:**

- The Pulsar Java client agent's pushback on Option 2 means the
  follow-ups.md D1 entry's recommendation was wrong. Future
  surface-lift commits cite this ADR for the design rationale,
  not the D1 entry.
- "Concrete `Surface<T, E>`" requires plumbing `Arc<E::ClientState>`
  through each surface type. Slightly more verbose than a GAT
  projection but easier to debug (no GAT lifetime ergonomics).

**Cost:**

- ~200 LOC for ADR-0026 (this file) + ~40 LOC index update in
  `specs/README.md` + `docs/follow-ups.md` cleanup.
- Follow-on commits per D1–D4 are tracked separately; this ADR
  only locks the decisions.

**Incompatible with:**

- Per-surface engine GATs (Option 3 from the follow-ups D1 menu).
  Revisit only if a third engine demands per-surface specialisation.

## Implementation status

All four decisions landed in commits between 2026-05-23 and
2026-05-24:

- **D4** — `xtask vendor-proto --rev <sha>` (commit `ac1420c`).
  Refreshes `crates/magnetar-proto/proto/{PulsarApi,PulsarMarkers}.proto`
  from a named upstream commit. Optional `--source <path>` reuses
  an existing clone; otherwise the helper does a shallow
  `--filter=blob:none` clone into a tempdir. Records the resolved
  SHA + committer date in `proto/SOURCE`; reruns codegen.
- **D3** — SASL/Athenz parity split (commit `96d6f74`). README
  parity matrix now distinguishes landed mechanisms (PLAIN ✅,
  Athenz pre-fetched role token ✅) from deferred (Kerberos/GSSAPI
  🟡, Athenz ZTS round-trip 🟡). ADR-0010 §Decision spells out
  the partial scope and rationale.
- **D2** — `crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`
  (commit `c23f6fd`). `BrokerWorkload` binds a sim `TcpListener`
  at the workload's assigned IP and replies to the minimum Pulsar
  wire subset (CONNECT/CONNECTED, PING/PONG, LOOKUP, PRODUCER,
  CLOSE_PRODUCER, CLOSE_CONSUMER); `ClientWorkload` drives
  `MoonpoolEngine<SimProviders>` against it. `sim_handshake_smoke`
  (1 iter) and `sim_handshake_sweep_16_seeds` (16 iter) run under
  `SimulationBuilder`. ADR-0024 carve-out recorded in xtask's
  `PARITY_EXEMPT_FILES` constant.
- **D1 phase 1** — `magnetar::engine::TransactionApi` extension
  trait + tokio delegate impl (commit `1258b89`).
- **D1 phase 2-4** — moonpool port + `MoonpoolClientState` impl +
  façade rewrite to
  `impl<E: Engine + TransactionApi> PulsarClient<E>` + 4+4 mirror
  tests + parity-status row flip (commit `ab9041b` + the
  intervening commits). Sub-PR template for the remaining seven
  surface lifts (Producer/Consumer foundational lift first, then
  Reader → TypedSchemas → MultiTopics → PartitionedProducer →
  PartitionedConsumer → PatternConsumer → TableView) documented
  in `docs/follow-ups.md` under "Façade surface bound to
  PulsarClient<MoonpoolEngine<P>>".

Each commit ships its own validation chain per ADR-0024.

## References

- [ADR-0019](0019-engine-scope-and-moonpool-parity.md) — engine
  scope contract; ADR-0026 is its phase-2 follow-up.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) —
  cross-runtime test policy; sim-chaos exemption rationale lives
  in §Exemptions.
- [ADR-0025](0025-engine-trait-task-and-timer-primitives.md) —
  phase 1 of the trait extension; ADR-0026 declines to grow it
  further.
- [`docs/follow-ups.md`](../../docs/follow-ups.md) — tracks the
  sub-PR work that lands the decisions in this ADR. The "Closed
  design decisions — see ADR-0026" preamble there is purely
  informational; the binding design lives in this file.
- Apache Pulsar Java client —
  `pulsar-client/src/main/java/org/apache/pulsar/client/impl/`
  ([PulsarClientImpl.java](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/PulsarClientImpl.java),
  [ProducerImpl.java](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ProducerImpl.java)).
- FoundationDB simulator —
  [testing.html](https://apple.github.io/foundationdb/testing.html),
  [Pierre Zemb's deep dive](https://pierrezemb.fr/posts/diving-into-foundationdb-simulation/),
  [SIGMOD'21 paper](https://www.foundationdb.org/files/fdb-paper.pdf).
- moonpool-sim 0.6 — [crates.io/crates/moonpool-sim](https://crates.io/crates/moonpool-sim/0.6.0),
  [github.com/PierreZ/moonpool](https://github.com/PierreZ/moonpool).
- PIP-466 (V5 surface) —
  [apache/pulsar/pip/pip-466.md](https://github.com/apache/pulsar/blob/master/pip/pip-466.md).
