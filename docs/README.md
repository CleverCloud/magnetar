# Magnetar — Documentation

This directory holds the long-form reference documentation for the magnetar workspace.
Top-level files ([`README.md`](../README.md), [`ARCHITECTURE.md`](../ARCHITECTURE.md), [`GUIDELINES.md`](../GUIDELINES.md), [`CONTRIBUTING.md`](../CONTRIBUTING.md), [`CLAUDE.md`](../CLAUDE.md)) remain the entry points for end users and contributors; this folder goes deeper.

## Index

| File                                       | Purpose                                                                                                                                                                                                                                                                                                                                      |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`moonpool-engine.md`](moonpool-engine.md) | Deterministic-simulation engine: `MoonpoolEngine<P>`, supervised reconnect, TLS, chaos test pack, differential equivalence harness; appendix on FoundationDB / TigerBeetle simulation patterns.                                                                                                                                              |
| [`memory-limit.md`](memory-limit.md)       | `MemoryLimitPolicy::{FailImmediately, ProducerBlock}` accounting (atomic CAS + Waker slab).                                                                                                                                                                                                                                                  |
| [`testing.md`](testing.md)                 | Test categories (unit, integration, deterministic chaos, differential, e2e/Docker) and how to run them.                                                                                                                                                                                                                                      |
| [`cli.md`](cli.md)                         | `magnetar` binary reference — global flags, `--version` banner, color policy, build-time metadata, and the full admin subcommand surface (V2 clusters / tenants / namespaces / topics / subscriptions / brokers / bookies / schemas, plus the V3 Functions / IO Sources / IO Sinks / Packages families). Every verb cites its REST endpoint. |
| [`pip-features.md`](pip-features.md)       | User-facing how-to for each PIP / auth surface: V5 client (PIP-466), shadow topics (PIP-180), replicated subscriptions (PIP-33), scalable topics (PIP-460, experimental), Athenz auth provider.                                                                                                                                              |
| [`observability.md`](observability.md)     | OpenTelemetry context propagation (`opentelemetry` feature): `traceparent`/`tracestate` wire contract, send-boundary injection, retry/DLQ re-injection, consumer extraction, and untrusted-peer security guidance (ADR-0053).                                                                                                                |
| [`logging.md`](logging.md)                 | Structured `tracing` logs: subscriber installation, level taxonomy, field glossary, no-secrets guarantees, CLI `-v` ladder pointer (ADR-0054).                                                                                                                                                                                               |
| [`follow-ups.md`](follow-ups.md)           | Consolidated open work tracker.                                                                                                                                                                                                                                                                                                              |

## Companion documents (top-level)

| File                                       | Purpose                                                                                                             |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| [`../README.md`](../README.md)             | Public-facing project README and Java parity matrix.                                                                |
| [`../ARCHITECTURE.md`](../ARCHITECTURE.md) | Architectural deep dive: sans-io rationale, driver loop, protocol state machine, schema canonicalisation, trackers. |
| [`../GUIDELINES.md`](../GUIDELINES.md)     | Binding project conventions: no-channels rule, I/O isolation, TLS, validation chain.                                |
| [`../CONTRIBUTING.md`](../CONTRIBUTING.md) | Toolchain, commit hygiene, branch naming.                                                                           |
| [`../specs/adr/`](../specs/adr/)           | Architecture Decision Records — one binding decision per file.                                                      |

## How to update

These documents are not auto-generated.
When a behavior, API, or architectural decision changes, edit the relevant file in the same changeset that lands the code.
Stale docs are bugs.

Concretely:

- A new PIP or Java-parity feature lands → update the parity matrix in [`../README.md`](../README.md#java-client-parity-matrix) and the engine-by-engine table in the same commit.
- An architectural decision changes → add a new numbered file in [`../specs/adr/`](../specs/adr/) AND update the index in [`../specs/README.md`](../specs/README.md).
- A new open follow-up surfaces (or one closes) → update [`follow-ups.md`](follow-ups.md).
