# ADR-0065 — Log rate-limiting / sampling is subscriber-side, not in the library

- **Status**: Accepted
- **Date**: 2026-06-15
- **Decider**: Florentin Dubois
- **Tags**: observability, logging, tracing, policy

## Context

[ADR-0054](0054-logging-policy.md) §7 ("Volume guidance") bounds log volume _structurally_: per-message records are confined to `trace!` / `debug!`, and `warn!` and above are bounded by churn (reconnects, refusals, lifecycle), never by send throughput.
It deliberately left one case open — its closing bullet flagged "Rate-limiting / sampling guidance for churn storms is an open follow-up (`docs/follow-ups.md`)".

The open case is a **churn storm**: the churn that bounds `warn!`-and-above can itself burst.
The canonical example is a broker-restart cascade — every supervised connection's reconnect loop emits one `warn!` per failed attempt at the single callsite in `crates/magnetar-runtime-tokio/src/driver.rs` (`"supervisor: reconnect attempt failed; will retry"`, fields `attempt` / `host` / `port` / `error`), so N reconnecting connections × M attempts collapse onto one callsite at an unbounded rate.

`tracing` has no built-in per-callsite rate limit.
The mechanism could live in one of two places:

- **Subscriber-side** — the application's `tracing` subscriber drops or samples events. Application-owned; zero library change.
- **Library-side** — magnetar rate-limits at each `warn!` callsite before emitting.

The library side is the wrong home, on this codebase's own rules:

- ADR-0054's Context establishes that `tracing` **emission** is write-only and state-machine-neutral — no wire byte, no `EventStream` entry, no scheduling decision depends on it, which is _why_ proto may emit at all (the ADR-0053 ambient-read objection does not transfer). A rate _limiter_ breaks that property: it carries mutable per-callsite state and, crucially, must **read a clock** to refill its budget.
- A clock read inside `magnetar-proto` violates [ADR-0011](0011-clock-injection-sans-io.md) (no `Instant::now()` / `SystemTime::now()` outside the allowlist; `check-no-internal-clock` enforces it). Threading an injected `now: Instant` to every log site to feed a limiter is exactly the "state not worth carrying for a log line" trade-off ADR-0054 §7 leans against, and it would expand the public surface (a limiter knob per builder) for a concern the subscriber gets for free.
- sozu — ADR-0054's source taxonomy — solves the same problem with render-time sanitization **in its own logger**, i.e. at the subscriber boundary, not at each call site.

The subscriber side has none of these costs: its clock (`Instant::now()`) and its state live in the operator's process, outside the sans-io core.
Per-callsite is also the _natural_ granularity for the storm: the whole cascade shares one `&'static` callsite, so a single token bucket caps it — and because the callsite keyspace is finite and static, the limiter needs no eviction.

## Decision

**Log rate-limiting / sampling for magnetar lives subscriber-side. The library ships no rate-limiting or sampling API and emits unchanged; it is the application's subscriber that drops or samples.**

- `magnetar-proto` and both runtime engines keep emitting per ADR-0054 §1–§7; no callsite gains a limiter, a clock read, or a sampling knob.
- The operator-facing recipe lives in [`docs/logging.md`](../../docs/logging.md) ("Rate-limiting and sampling"), as three tiers an operator composes:
  1. **Static** — `EnvFilter` per-target suppression (raise a known-noisy target's floor, e.g. `RUST_LOG=info,magnetar_runtime_tokio::driver=error`). Cheapest; coarse (silences the whole target).
  2. **Dynamic** — a dependency-free per-callsite token-bucket `tracing_subscriber::Layer` keyed by `metadata.callsite()`, calling `Instant::now()` in the subscriber. Targets exactly the storming callsite; bounded keyspace, no eviction.
  3. **Fleet** — collector-side sampling / dedup (OTLP collector / Vector / Loki) for storms that aggregate across processes, which an in-process limiter cannot see.
- Operational caveat the recipe states: dropping the _log line_ hides the _signal_; pair rate-limiting with a metric / alert on the underlying condition (reconnect rate), not on the line.

This **amends ADR-0054 §7** — it resolves §7's own open follow-up.
ADR-0054 is **not superseded**: every other clause stands.

## Consequences

- **Easier:** operators tame churn storms with stock `tracing-subscriber` primitives, scaled to need (one env var → one small `Layer` → a collector); magnetar's emission contract (ADR-0054) stays simple and clock-free.
- **Harder:** nothing in the library; the burden is a few lines of subscriber wiring in the application, documented and verified to compile.
- **Costs:** none in the workspace — no new dependency, no API, no test surface (docs-only). The recipe's correctness is pinned by a throwaway-crate compile + behavior check, not a workspace test.
- **Incompatible with:** any future library-side rate-limit / sampling API (it would reopen the ADR-0011 clock-read and per-callsite-state costs this ADR rejects); a limiter that reads a clock inside `magnetar-proto`.

## References

- [ADR-0054](0054-logging-policy.md) — logging policy; §7 ("Volume guidance") amended by this ADR; its Context establishes emission is write-only / state-machine-neutral.
- [ADR-0011](0011-clock-injection-sans-io.md) — clock-injection rule a library-side limiter would violate (`check-no-internal-clock`).
- [ADR-0053](0053-otel-context-propagation.md) — ambient-read boundary; the emission-vs-read distinction is reused here.
- `docs/logging.md` — operator-facing recipe (static `EnvFilter` → per-callsite `Layer` → collector-side).
- `crates/magnetar-runtime-tokio/src/driver.rs` — the reconnect-cascade `warn!` callsite the storm shares.
- sozu (`github.com/sozu-proxy/sozu`, HEAD `ef93a834`) — render-time sanitization in its own logger (subscriber-boundary precedent).
