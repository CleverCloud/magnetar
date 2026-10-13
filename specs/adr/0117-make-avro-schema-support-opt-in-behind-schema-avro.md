# ADR-0117 — Make Avro schema support opt-in behind `schema-avro`

- **Status**: Accepted
- **Date**: 2026-10-13
- **Decider**: Florentin Dubois
- **Tags**: schemas, cargo-features, dependencies, semver, breaking-change, ci

## Context

`magnetar-proto` depended on `apache-avro` unconditionally, so every consumer compiled it: the `magnetar-driver` façade, `magnetar-admin`, and `magnetarctl`, which never builds a typed schema.
Only one type needs it.
`AvroSchema` (`crates/magnetar-proto/src/schema/avro.rs`) parses the writer schema, advertises its Avro parsing canonical form as `schema_data`, and encodes and decodes datums.
`JsonSchema` also advertises an Avro record definition, but it builds that document with `serde_json` and `schemars` and never links `apache-avro`; Bytes, String, the primitive and temporal schemas, Protobuf, ProtobufNative, KeyValue, AutoConsume and AutoProduceBytes do not either.

PR #878 puts `AvroSchema` behind a `schema-avro` feature that is off by default.
Its description reports a basic build dropping from 209 to 167 crates.
Measured on this branch on 2026-10-13 with `cargo tree -e normal` (unique crate versions), switching the feature on adds 33 crates to a default build:

| Package           | Default | With `schema-avro` | `origin/main` before the PR |
| ----------------- | ------- | ------------------ | --------------------------- |
| `magnetar-proto`  | 60      | 93                 | 93                          |
| `magnetar-driver` | 107     | 140                | 143                         |
| `magnetarctl`     | 162     | not enabled        | 197                         |

The difference between the last two columns of `magnetar-driver` (3 crates) is the unused dependency trimming that PR #878 does in the same changeset; it is unrelated to this decision.

The cost is a source-breaking change.
[ADR-0066](0066-release-1-0-0-first-stable.md) puts every ✅ row of the parity matrix inside the 1.x stability promise, `AvroSchema` included, and until now it was always compiled.
Code that names `magnetar_proto::schema::AvroSchema` (the façade re-exports the crate as `magnetar::proto`) stops compiling with a default feature set.
`docs/follow-ups.md` ("API stability stance") accepts a breaking change when it carries a `BREAKING CHANGE:` footer, a `CHANGELOG.md` entry, and a statement of whether the ergonomic façade is affected or only the low-level `magnetar-proto` API; here the façade is affected.

Alternatives weighed:

1. **Keep `apache-avro` unconditional.**
   No break, and every consumer keeps paying for a codec that most clients, `magnetarctl` first, never use.
2. **Make `schema-avro` a default feature of the façade.**
   Façade users would not break, but they are the consumers the saving is for, so the default build would gain nothing.
   A default feature also cannot be removed later without another breaking change, whereas an opt-in feature can be added to the defaults.
3. **Move `AvroSchema` into its own crate.**
   It would avoid the feature but add a published crate, a version-lockstep entry and a new import path for the same type, a larger break than a feature flag.

## Decision

- `AvroSchema` and its `apache-avro` dependency are compiled only when `magnetar-proto`'s `schema-avro` feature is on.
  The `magnetar-driver` façade forwards it as `schema-avro = ["magnetar-proto/schema-avro"]`.
  Both leave it off by default, and `magnetarctl` does not enable it.
- Every other schema type stays unconditional, `JsonSchema` included.
  The wire behaviour of `AvroSchema` does not change.
- The change is recorded as a breaking change to the façade: a `BREAKING` entry in `CHANGELOG.md` under `[Unreleased]`, and a `BREAKING CHANGE:` footer on the commit that adds it.
  Enabling the feature is the whole migration: `magnetar-driver = { version = "…", features = ["schema-avro"] }`, or the same feature on a direct `magnetar-proto` dependency.
  rustc names the feature when the import fails (`the item is gated behind the schema-avro feature`).
- Tests that need Avro keep running, in two ways.
  `--all-features`, which every CI job that compiles the workspace passes, enables the feature.
  The two tests that are not about the `AvroSchema` type link it on their own, so they also run with no feature: the `JsonSchema` unit test that checks `schema_data` parses as Avro (`apache-avro` as a `magnetar-proto` dev-dependency), and the Avro e2e test in `crates/magnetar/tests/e2e_schemas_extended.rs` (`magnetar-proto/schema-avro` as a façade dev-dependency).
  Neither is gated with `#[cfg]`, so ADR-0046's rule that e2e tests carry no compile-time feature gate holds.
- `--all-features` cannot see a missing `#[cfg(feature = "schema-avro")]` gate, so `ci.yml` also lints and documents the default build: `cargo clippy -p magnetar-proto --all-targets`, `cargo clippy -p magnetar-driver --lib` and `cargo doc -p magnetar-proto --no-deps`, each as its own invocation so a dev-dependency of one package cannot switch the feature back on for another.
  Documentation of `AvroSchema` in always-compiled docs is a code span, never an intra-doc link.
- `magnetar-proto` builds its docs.rs documentation with `schema-avro`, so the type stays in the published API reference.

## Consequences

- A default dependency graph 33 crates smaller for `magnetar-proto` and the façade, and a faster cold build for everything that does not need Avro.
- A source break on 1.x for every user of `AvroSchema`.
  ADR-0066 says a post-1.0.0 breaking change requires a major-version bump, and `[Unreleased]` already carries other `BREAKING` entries; which version the next release takes is a release decision this ADR does not make.
  ADR-0066's stability promise is amended in one respect only: `AvroSchema` stays stable but requires `schema-avro`.
- Cargo unifies features across a build, so any crate that enables `schema-avro` enables it for every dependent in that build.
  In this workspace `magnetar-differential` enables it on its `magnetar-proto` dependency, and it is a dev-dependency of the moonpool runtime, so workspace-wide commands (`cargo check --workspace`) and the moonpool test binaries always compile Avro.
  Only a `-p`-scoped invocation of a package that does not reach `magnetar-differential` shows the real default build, which is why the default-feature CI cells above are one package each.
- The dependency-trimming commit in the same PR (`1597d5b`) is separate from this decision and recorded in `CHANGELOG.md`.

## References

- PR #878 — `schema-avro` and the unused-dependency trimming (hcaumeil).
- `crates/magnetar-proto/Cargo.toml`, `crates/magnetar/Cargo.toml` — the `schema-avro` features.
- `crates/magnetar-proto/src/schema/mod.rs` — the `#[cfg(feature = "schema-avro")]` gates.
- `.github/workflows/ci.yml` — the default-feature clippy and doc cells.
- [ADR-0035](0035-pluggable-crypto-provider.md) — the other feature set the façade defines.
- [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) — e2e tests carry no compile-time feature gate.
- [ADR-0066](0066-release-1-0-0-first-stable.md) — the stability promise this amends.
- [ADR-0086](0086-inject-now-into-proto-latency-recording.md) — worked example of a recorded breaking change.
