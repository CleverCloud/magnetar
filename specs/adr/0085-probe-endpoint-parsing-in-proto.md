# ADR-0085 — Move health-probe endpoint parsing into `magnetar-proto`

- **Status**: Accepted
- **Date**: 2026-07-29
- **Decider**: Florentin Dubois
- **Tags**: pip-121, ha, failover, sans-io, health-probe, bit-flip-survivability, robustness

## Context

[ADR-0023](0023-health-probe-trait-extraction.md) moved the `HealthProbe` **trait** into `magnetar-proto` and left each engine to host its own implementation, including its own parse of the `endpoint` string into the `host:port` authority it dials.

Both engines then implemented that parse identically — literally byte-for-byte:

```rust
let stripped = endpoint
    .strip_prefix("pulsar+ssl://")
    .or_else(|| endpoint.strip_prefix("pulsar://"))
    .unwrap_or(endpoint);          // <- unrecognised scheme falls through UNSTRIPPED
let auth = stripped.split('/').next().unwrap_or(stripped);
```

`crates/magnetar-runtime-tokio/src/auto_cluster_failover.rs` and `crates/magnetar-runtime-moonpool/src/auto_cluster_failover.rs` carried that copy, and both carried the same defect.

On an input containing `"://"` whose scheme is neither Pulsar scheme — a bit-flipped `"ptlsar://broker:6650"`, the exact shape moonpool-sim's chaos produced for issue #364 — the `unwrap_or(endpoint)` returns the ORIGINAL unstripped string, and the subsequent `split('/').next()` truncates it into the nonsense authority `"ptlsar:"`.
The probe then handed that fabricated target to `tokio::net::lookup_host` / `NetworkProvider::connect`.

Two properties made this low-severity but worth fixing properly:

- **The verdict was already correct, by accident.** The fabricated authority does not resolve, the dial fails, and `spawn_probe` maps a failed dial to `verdict = false`. So the corrupted endpoint already read unhealthy. What was wrong was _how_: I/O against a target the client invented, rather than refusal of input it could not interpret. (Contrast `Client::proxy_broker_authority`, whose corrupted value flowed into `CommandConnect.proxy_to_broker_url` on the wire — that one was a routing defect and was fixed under issue #364, commit `db96010`.)
- **The two copies rotted in lockstep**, so no cross-engine differential test could ever have caught it. Symmetric duplication defeats the equivalence oracle that [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) relies on.

A second, adjacent gap surfaced while fixing the first: a scheme-carrying endpoint with no explicit port (`pulsar://broker.local`) yielded the port-less authority `"broker.local"`, which the dialer then rejects — a **guaranteed** probe false-negative, not merely an accidental one.
`proxy_broker_authority` (`crates/magnetar-runtime-moonpool/src/client.rs`) already synthesises `:6650` / `:6651` for exactly this case, so the two siblings disagreed.

Filed as the "Health-probe authority scheme truncation" entry in `docs/follow-ups.md` (removed there when this ADR landed, per that file's convention), found via the `strip_prefix("pulsar` reference sweep mandated by [ADR-0055](0055-bit-flip-survivability-model.md).

## Decision

The canonical parse of a `HealthProbe` endpoint is part of the **sans-io contract**, not per-engine I/O policy.

- Add `magnetar_proto::probe_authority(&str) -> Option<String>` in `crates/magnetar-proto/src/health_probe.rs`, beside the trait whose module doc already specifies the endpoint contract.
  Both engines' private `authority()` becomes a one-line delegation; neither retains a parser of its own.
- An input containing `"://"` whose scheme is neither `pulsar://` nor `pulsar+ssl://` returns `None`.
  Per the `HealthProbe` contract an unparseable endpoint is reported unhealthy, so a corrupted URL now costs one probe verdict and **zero I/O**.
- A scheme-carrying endpoint with no explicit port resolves to that scheme's default (`6650` / `6651`), matching `proxy_broker_authority` arm-for-arm.
- A genuine scheme-less bare `host:port` is unaffected and still round-trips verbatim.
  This is the boundary the fix must not over-correct across, so it is pinned explicitly by the differential test, not only by the accept-side unit tests.
- The empty-authority check runs **before** default-port synthesis, so `"pulsar://"` returns `None` rather than the garbage authority `":6650"`.

The implementation is a hand-rolled scan with no `url` crate, following the `extract_pulsar_host` precedent at `crates/magnetar-proto/src/conn_types.rs:141`, so `magnetar-proto` keeps the zero-I/O dependency surface [ADR-0004](0004-sans-io-protocol-core.md) requires (`cargo run -p xtask -- check-no-io-deps`).

### Known limitation, accepted deliberately

Default-port synthesis triggers on "the authority contains no `:`", so a **bracketed IPv6 literal with no port** (`pulsar://[::1]`) gets no synthesised port.
This is inherited from `proxy_broker_authority`, whose behaviour this function mirrors; a one-sided divergence between the two would be worse than the shared gap.
`pulsar://[::1]:6650` — the form that appears in real deployments — round-trips correctly.
Pinned by `probe_authority_leaves_bracketed_ipv6_untouched` so it stays a recorded decision rather than an accident.

### Scope boundary

`proxy_broker_authority` / `direct_broker_authority` are **not** refactored onto this helper here.
They return `Result<String, ClientError>` with caller-specific error text and sit on the lookup/routing path — a different blast radius than a probe verdict.
Filed as an open item in `docs/follow-ups.md` instead.

## Consequences

**Positive**

- The parse rule is single-sourced. A future engine cannot re-fork it silently, and the differential test asserts both engines agree on refusal and on acceptance.
- A corrupted probe endpoint costs zero syscalls instead of a doomed DNS lookup plus connect.
- Port-less `pulsar://host` endpoints now probe correctly instead of always reading unhealthy.
- `probe_authority` is public, so third-party `HealthProbe` implementors get the same parse for free rather than re-deriving the bug.

**Negative**

- `magnetar-proto`'s public surface grows by one function; it is additive and non-breaking, but it is now a compatibility commitment.
- ADR-0023's "every engine hosts its own implementation" boundary moves: the parse is shared, only the I/O stays per-engine. That is the refinement this ADR records, not a reversal — the trait placement rationale in ADR-0023 is untouched.

**Neutral**

- The IPv6 gap is carried forward, not introduced. Closing it means changing `proxy_broker_authority` in the same motion so the two stay identical.

### Alternatives considered

- **Duplicate the `contains("://")` guard into both engines.** Smallest diff, no new public API, no ADR. Rejected: it preserves the exact duplication that produced the bug, and leaves ADR-0024 layer (a) with no honest home — the changed code would not be in `magnetar-proto`, so any proto unit test would be decorative.
- **Return `Result<_, E>` with a typed proto error.** Rejected: `None` already means "unprobeable → unhealthy" in the `HealthProbe` contract, and the sole caller in each engine discards the reason after logging it.
- **Reuse the private `extract_pulsar_host`** (`conn_types.rs:141`). Rejected: it drops the port and applies an IPv6-bracket carve-out tuned for allow-list host matching, which is a different job from producing a dialable authority.

## References

- `docs/follow-ups.md` — the "Health-probe authority scheme truncation" entry this closes (removed on landing; this ADR is its post-implementation reference) and the "Broker-URL authority parser unification" entry it opens.
- `crates/magnetar-proto/src/health_probe.rs` — `probe_authority` + ADR-0024 layer (a) unit tests.
- `crates/magnetar-runtime-{tokio,moonpool}/src/auto_cluster_failover.rs` — the delegating `authority()` and the in-src regression tests.
- `crates/magnetar-runtime-{tokio,moonpool}/tests/probe_corrupted_scheme.rs` — layers (b) / (c): the tracing witness proving no I/O is attempted.
- `crates/magnetar-differential/tests/probe_authority_equivalence.rs` — layer (d): both engines agree on refusal AND on acceptance.
- `crates/magnetar/tests/e2e_probe_corrupted_scheme.rs` — end-to-end failover away from a corrupted primary.
- [ADR-0023](0023-health-probe-trait-extraction.md) — refined here.
- [ADR-0055](0055-bit-flip-survivability-model.md) — the sweep that found this.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the test layers shipped with it.
