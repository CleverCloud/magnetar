# ADR-0027 — `MoonpoolEngine::ClientState` is the runtime `Client<P>`

- **Status**: Accepted
- **Date**: 2026-05-23
- **Decider**: Florentin Dubois
- **Tags**: engines, moonpool, façade, clientstate, traits

## Context

[ADR-0019](0019-engine-scope-and-moonpool-parity.md) §gate (e) shipped
`PulsarClient<MoonpoolEngine<P>>` with a custom `MoonpoolClientState`
wrapper (`shared: Arc<ConnectionShared>` + `driver: Mutex<Option<DriverHandle>>`)
as the engine's `ClientState`, while the runtime crate's
`magnetar_runtime_moonpool::Client<P>` carried the same fields plus a
`PhantomData<fn() -> P>` marker. Two structurally-identical types
side-by-side, with one delivering all the surface trait impls
(`SubscribeApi`, `CreateProducerApi`, `TransactionApi`, …) and the
other plugged into `MoonpoolEngine::ClientState`.

The consequence: even though the runtime `Client<P>` implemented every
extension trait that the façade builders dispatch through
(`ConsumerBuilder::subscribe`, `ProducerBuilder::create`,
`ReaderBuilder::create`), the bound `E::ClientState: SubscribeApi`
(etc.) was *not* satisfied for `MoonpoolEngine<P>` because
`MoonpoolClientState` had no such impls. The façade-side workaround
documented in ADR-0019 was "drive the moonpool runtime directly via the
`shared()` accessor". This was acceptable while the moonpool surface
was a v0.1.0 follow-up (M5–M8), but the user has now decided the
moonpool engine is a first-class peer of tokio in the project's
production-grade scope.

The tokio engine never had this asymmetry: `TokioEngine::ClientState`
**is** `magnetar_runtime_tokio::Client`. The trait impls on the
runtime `Client` automatically satisfy the bounds. The moonpool side
was the odd one out.

## Decision

`MoonpoolEngine<P>::ClientState` is
`magnetar_runtime_moonpool::Client<P>` directly. Mirror of
`TokioEngine::ClientState = magnetar_runtime_tokio::Client`. The
parallel `MoonpoolClientState` struct is removed.

Mechanics:

- `magnetar_runtime_moonpool::Client::<P>::from_parts(shared, driver)`
  becomes the public constructor the façade uses to wrap an
  engine-side `(shared, driver)` pair.
- The `TransactionApi` impl previously bound to `MoonpoolClientState`
  is rewritten as
  `impl<P: Providers + Send + Sync + 'static> TransactionApi for
  magnetar_runtime_moonpool::Client<P>`, using `self.shared()` (the
  public accessor) in place of the now-private field access.
- `PulsarClient::<MoonpoolEngine<P>>::{is_connected, is_closed,
  shared, take_driver, close}` delegate to the inherent methods on
  the runtime `Client<P>` (the runtime already exposes the same
  surface that the façade used to re-implement against the raw
  fields).
- New convenience constructors on `PulsarClient<MoonpoolEngine<P>>`:
  - `from_moonpool(shared, driver)` (preserved signature).
  - `from_runtime_client(Client<P>)` for callers that own the runtime
    client directly.
  - `runtime_client(&self) -> &Client<P>` accessor.

## Consequences

- **`producer()` / `consumer()` / `reader()` on
  `PulsarClient<MoonpoolEngine<P>>` work end-to-end** — calling
  `.create()` / `.subscribe()` on the builders dispatches through the
  same `CreateProducerApi` / `SubscribeApi` traits the tokio engine
  uses. A compile-time witness lives at
  [`crates/magnetar/src/moonpool_client.rs`](../../crates/magnetar/src/moonpool_client.rs)
  (`moonpool_builder_dispatch_compiles`).
- **ADR-0019 §gate (e) is partially superseded.** The "façade surface
  stays bound to `PulsarClient<TokioEngine>`" carve-out no longer
  applies to the three engine-generic builder entry points. The
  v0.1.0 Java parity matrix is still satisfied by the tokio engine —
  this ADR is only about *which type implements the façade traits*,
  not about *which engine carries production load*.
- **No new dependencies.** The trait impls already existed on
  `Client<P>`; only the engine's `type ClientState` pointer moved.
  `magnetar-proto`'s I/O-free guarantee is unchanged.
- **Test count parity unchanged** (95 ↔ 95 on
  `xtask check-runtime-test-parity`). The ClientState lift only
  reorganises which type the trait impls hang on; the per-runtime test
  surface didn't grow.
- **Surfaces not yet lifted on moonpool** (TypedSchemas with the full
  encryptor / scheduling helper set, MultiTopicsConsumer's pause /
  resume / auto-update, PatternConsumer, TableView read paths, the
  PIP-4 encryption setter) are still tokio-only — their inherent
  methods reference encryptor and scheduling primitives that haven't
  been ported to moonpool. Those lifts remain individually scoped per
  ADR-0026 §D1, but no longer carry the ClientState mismatch as an
  additional blocker.
- **Documentation drift fixed in the same changeset**:
  `crates/magnetar/src/moonpool_client.rs` rewrites the
  module-doc reference to ADR-0019's "façade surface stays bound to
  tokio" so it accurately describes the post-lift surface.

## References

- [ADR-0019](0019-engine-scope-and-moonpool-parity.md) §gate (e) —
  the partially-superseded scope cut.
- [ADR-0025](0025-engine-trait-task-and-timer-primitives.md) — the
  `Engine` trait surface this ADR adjusts the type-pointer of.
- [ADR-0026](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
  §D1 — the per-surface extension trait family
  (`SubscribeApi`/`CreateProducerApi`/`ConsumerApi`/`ProducerApi`/
  `TransactionApi`) this ADR makes uniformly dispatchable from the
  façade.
- `crates/magnetar/src/moonpool_client.rs` — new constructors +
  delegates + compile-time witness.
- `crates/magnetar/src/engine.rs` — `Engine for MoonpoolEngine<P>`
  type pointer + `TransactionApi for Client<P>`.
- `crates/magnetar-runtime-moonpool/src/client.rs` — `from_parts` +
  `take_driver` constructors exposed on the runtime `Client<P>`.
