# ADR-0091 — Canonical broker authorities with caller-selected scheme-less defaults

- **Status**: Accepted (amends [ADR-0087](0087-unify-broker-url-authority-parsers.md) — the Tokio DIRECT adapter and Moonpool DIRECT default now share the proto implementation; its proxy and driver contracts remain binding)
- **Date**: 2026-07-31
- **Decider**: Florentin Dubois
- **Tags**: broker-authority, direct-routing, dns, tokio, moonpool, adr-0024, adr-0087

## Context

[ADR-0087](0087-unify-broker-url-authority-parsers.md) moved three broker-authority parsers onto `magnetar_proto::probe_authority`, but deliberately left Tokio's `parse_direct_broker_url` independent because it returns a `ParsedUrl` rather than an authority string.
Its table-driven audit recorded two representation differences: Tokio supplies its bootstrap scheme's port for a scheme-less host, while `probe_authority` preserves that host without a port; Tokio also rejected malformed bracket syntax that the old helper returned verbatim.

The first difference exposed a runtime defect rather than a cosmetic seam.
Moonpool's DIRECT path used `probe_authority` without a fallback, passed a portless physical target to its pool, and reached `Transport::connect_with_resolver`, whose `split_host_port` requires `host:port` before invoking the configured resolver.
The executable failure chain was:

```text
direct_broker_authority preserves "broker-b.internal"
  -> resolve_direct_broker forwards the portless physical target
  -> Transport::connect_with_resolver rejects it before resolver dispatch
  -> Engine(Config("invalid host:port literal \"broker-b.internal\""))
```

Tokio already requested `("broker-b.internal", 6650)` and reached the resolved broker under the identical plaintext topology.
The two runtime integrations therefore disagreed after consuming the same `LookupOutcome::Connect`.

The audit also exposed a validation boundary worth making explicit.
A broker authority parser must distinguish an absent port from an invalid port or malformed IPv6 brackets before a dial target is cached.
DNS-label validity remains the resolver's job.

## Decision

`magnetar-proto` owns one canonical normalizer:

```rust
pub fn broker_authority(
    endpoint: &str,
    schemeless_default_port: Option<u16>,
) -> Option<String>
```

`probe_authority(endpoint)` remains public with its existing signature and delegates to `broker_authority(endpoint, None)`.
The optional argument applies only when the input carries no recognized Pulsar scheme and no explicit port.
`broker_endpoint_scheme(endpoint)` exposes the same canonical classification to adapters that must retain scheme identity, and returns `BrokerEndpointScheme::{Pulsar, PulsarTls, Schemeless}`.
Recognized URI schemes are matched ASCII-case-insensitively, preserving Tokio's prior `url::Url` behavior for inputs such as `PULSAR://broker`.

### Precedence and rejection contract

| Input shape                                                                              | Result                     |
| ---------------------------------------------------------------------------------------- | -------------------------- |
| Recognized Pulsar scheme in any ASCII case plus explicit port                            | Preserve the explicit port |
| `pulsar://host`                                                                          | `host:6650`                |
| `pulsar+ssl://host`                                                                      | `host:6651`                |
| Scheme-less host plus explicit port                                                      | Preserve the explicit port |
| Scheme-less host without a port, fallback `Some(port)`                                   | Append the supplied port   |
| Scheme-less host without a port, fallback `None`                                         | Preserve the portless host |
| Unknown `://` scheme                                                                     | Reject                     |
| Empty input or recognized scheme without an authority                                    | Reject                     |
| Invalid explicit port, malformed bracketed IPv6, or unbracketed multiple-colon authority | Reject                     |

Bracketed IPv6 literals follow the same precedence.
Trailing paths are removed before validation, preserving the existing broker-URL behavior.
The helper validates structure only: DNS labels and reachability remain resolver concerns.

### Tokio adapter

`parse_direct_broker_url` calls `broker_authority` with `bootstrap_scheme.default_port()`, consumes `broker_endpoint_scheme` to preserve an explicit recognized scheme when present, otherwise applies the bootstrap scheme, reconstructs one Pulsar URL, and returns the existing `ParsedUrl`.
It no longer owns a separate authority rule.

Its public return shape and every client façade signature stay unchanged.
Malformed authorities now fail with an accurate “not a usable authority” error before dialing.

### Moonpool pool

`ConnectionFactory` records `schemeless_default_port`.
The only current pool-building constructor is `connect_plain_supervised`, so it records `6650`.
`resolve_direct_broker` supplies that value to `direct_broker_authority` before bootstrap comparison or pool insertion.

Proxy routing continues to use `probe_authority` without a fallback because its logical authority is forwarded on the wire rather than used as the DIRECT physical dial target.
This decision does not introduce a supervised TLS constructor or a TLS-capable Moonpool connection pool.

## Compatibility

`broker_authority`, `broker_endpoint_scheme`, and `BrokerEndpointScheme` are additive public `magnetar-proto` APIs.
`probe_authority`, Tokio's `ParsedUrl`, both runtime clients, and the ergonomic `magnetar` façade keep their signatures.
Explicit schemes and ports retain their precedence.
Mixed- or uppercase Pulsar schemes remain accepted, matching RFC 3986 and Tokio's pre-refactor URL parser.

Inputs with invalid explicit ports, malformed brackets, or structurally ambiguous unbracketed IPv6 are rejected earlier instead of reaching a resolver or dialer.
That is correctness hardening, not a façade break, and requires no `BREAKING CHANGE:` footer.

## Alternatives rejected

| Alternative                                              | Why rejected                                                                                                                                       |
| -------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Keep Tokio independent and rely on its equivalence table | An audit detects drift only after a test edit; it does not give Moonpool the missing default or establish one implementation.                      |
| Move `ParsedUrl` into `magnetar-proto`                   | It couples the sans-io authority contract to a runtime representation and changes more public surface than the defect requires.                    |
| Let Moonpool's transport invent a port                   | The transport receives only a physical string, too late to preserve scheme/default precedence and before correct pool identity can be established. |
| Add a Moonpool TLS pool now                              | No such constructor exists; adding it is a separate transport capability, not required for the plaintext defect.                                   |
| Validate complete DNS syntax in proto                    | Internationalized names and resolver-specific behavior are outside structural authority normalization.                                             |

## Verification evidence

The Moonpool regression was observed red before the production change:

```text
cargo test -p magnetar-runtime-moonpool --test lookup_direct_multi_broker portless
-> Engine(Config("invalid host:port literal \"broker-b.internal\""))
```

The client-level differential witness independently showed Tokio's successful `("broker-b.internal", 6650)` resolution against Moonpool's configuration error.
After default propagation, both unchanged commands passed.
Focused helper, Tokio integration, Moonpool integration, and differential suites also passed without ignored tests.

The test layers are:

- proto unit tables for precedence and structural rejection;
- Tokio resolver integration against two in-process brokers, including an uppercase scheme;
- Moonpool resolver integration against the same topology and scheme cases;
- client-level differential comparison of requested authority and producer routing;
- a one-container facade e2e whose lookup stub advertises a bare hostname, whose recording resolver observes port 6650, and whose resolved connection reaches a real Docker Pulsar broker.

## Consequences

**Easier.** All runtime authority adapters share one structural rule, and a configured Moonpool resolver can now receive a portless broker hostname with the protocol default already attached.

**Harder.** Callers that need scheme-less defaulting must choose the default explicitly.
Moonpool's pool stores one extra `u16`, making its current plaintext-only posture visible rather than implicit.

**Residual risk.** Moonpool still has no supervised TLS pool, so only the existing plaintext constructor supplies a scheme-less default there.
Proxy behavior remains intentionally asymmetric with Tokio's warn-and-forward proxy path, as recorded by ADR-0087.

## References

- `crates/magnetar-proto/src/health_probe.rs` — canonical `broker_authority` and compatibility wrapper.
- `crates/magnetar-runtime-tokio/src/client.rs` — `parse_direct_broker_url` adapter.
- `crates/magnetar-runtime-moonpool/src/client.rs` — DIRECT normalization and pool construction.
- `crates/magnetar-runtime-moonpool/src/pool.rs` — stored scheme-less default.
- `crates/magnetar-runtime-{tokio,moonpool}/tests/lookup_direct_multi_broker.rs` — runtime resolver witnesses.
- `crates/magnetar-differential/tests/lookup_direct_multi_broker_equivalence.rs` — client-level parity witness.
- `crates/magnetar/tests/e2e_moonpool_direct_broker_authority.rs` — facade-level portless resolver-to-Docker-broker witness.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — required cross-runtime test layers.
- [ADR-0087](0087-unify-broker-url-authority-parsers.md) — parser unification this decision completes.
