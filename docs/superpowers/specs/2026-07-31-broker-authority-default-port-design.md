# Broker Authority Default-Port Unification Design

**Status:** Approved on 2026-07-31.

## Context

Vault recall for `magnetar broker authority parser default port tests ADR follow-up item 8` returned no matching note.
The repository sources and accepted ADRs are therefore the complete authority for this design.

`docs/follow-ups.md` item 8 records `magnetar_runtime_tokio::client::parse_direct_broker_url` as an audited fifth implementation of the broker-URL authority rule.
The entry describes one deliberate divergence: a scheme-less, portless DIRECT broker name takes the Tokio bootstrap scheme's default port, while `magnetar_proto::probe_authority` preserves it without a port.

The divergence is not representation-only on Moonpool.
`direct_broker_authority` currently delegates to `probe_authority`, `resolve_direct_broker` forwards its string as the physical dial target, and `Transport::connect_with_resolver` requires that target to split as `host:port`.
With a configured resolver, a portless target is rejected before resolution.

CAUSE: `crates/magnetar-runtime-moonpool/src/client.rs:1588` delegates without a scheme-less fallback -> `crates/magnetar-runtime-moonpool/src/client.rs:743` forwards the portless authority -> `crates/magnetar-runtime-moonpool/src/transport.rs:907` requires `host:port` -> SYMPTOM: Moonpool cannot dial the portless DIRECT target that Tokio accepts.

## Requirements

- Keep scheme-less, portless broker names supported.
- Derive port 6650 from a plaintext bootstrap and port 6651 from a TLS bootstrap wherever the runtime already supports a pooled DIRECT path.
- Let an explicit `pulsar://` or `pulsar+ssl://` scheme override the bootstrap fallback.
- Keep a bare explicit `host:port` or `[IPv6]:port` unchanged.
- Preserve Tokio's `ParsedUrl` return type and public façade.
- Single-source scheme recognition, path trimming, default-port synthesis, and structural authority validation in `magnetar-proto`.
- Reject unusable broker authorities before a dial is attempted.
- Add no dependency.
- Update implementation, tests, changelog, governing ADRs, and the follow-up tracker together.

## Approaches Considered

### Canonical string normalizer with a caller fallback

Extend the existing hand-written proto normalizer so it accepts an optional default port for scheme-less input.
`probe_authority` remains the compatibility wrapper that supplies no fallback.
Runtime DIRECT paths supply the bootstrap default and retain their caller-specific output shapes.

This is the selected approach.
It removes the duplicated rule with the smallest public surface and keeps `magnetar-proto` free of new dependencies.

### Canonical structured URL type in `magnetar-proto`

Move scheme, host, and port parsing into a new public proto type and convert that type into Tokio's `ParsedUrl` and Moonpool's authority string.
This avoids Tokio's final structural parse but creates a larger compatibility commitment and duplicates part of the mature `url` crate's job in a hand-written parser.

Rejected because item 8 does not require replacing `ParsedUrl`; it requires one authority rule.

### Moonpool-only default-port fix

Supply 6650 locally when Moonpool sees a portless target and leave the Tokio audit unchanged.
This fixes the discovered symptom but preserves the fifth implementation and leaves future drift possible.

Rejected because it cannot close item 8 by construction.

## Decision

Add a public `broker_authority(endpoint: &str, schemeless_default_port: Option<u16>) -> Option<String>` function to `magnetar-proto`.
It owns the complete authority-normalization rule.

Keep `probe_authority(endpoint)` as a source-compatible wrapper equivalent to `broker_authority(endpoint, None)`.
Existing health probes and stricter callers retain their no-fallback contract.

The selected default follows this precedence:

1. `pulsar://` selects 6650.
2. `pulsar+ssl://` selects 6651.
3. Scheme-less input uses `schemeless_default_port` when one was supplied.
4. Scheme-less input without a fallback remains portless.

An explicit valid port always wins.
Trailing path segments remain trimmed before authority validation.

## Authority Validation

The canonical normalizer rejects:

- input carrying an unrecognised `://` scheme;
- an empty input or recognised scheme with no authority;
- an unterminated or otherwise unusable bracketed IPv6 authority;
- an explicit empty, non-numeric, or out-of-range port;
- an unbracketed authority containing multiple colons.

A bracketed IPv6 literal retains its brackets in the normalized authority.
The standard library validates the IPv6 address body without adding a crate.
DNS-name syntax and reachability remain the resolver and dialer's responsibility.

## Runtime Data Flow

### Tokio

`parse_direct_broker_url` calls `broker_authority` with `Some(bootstrap_scheme.default_port())` before constructing a dial target.
It determines the effective `Scheme` from an explicit recognised scheme or the bootstrap scheme for a bare input, reconstructs one normalized Pulsar URL from the canonical authority, and lets `ParsedUrl::parse` produce the existing `{ scheme, host, port }` result.

The two scheme-prefix comparisons select Tokio's transport posture only.
They do not reimplement path trimming, authority validation, or port synthesis.

Every canonical rejection maps to `ClientError::Other` with a message that describes an unusable authority and the accepted input shapes.
The message must not claim that `pulsar://` carries an unrecognised scheme.

### Moonpool

The pool factory records the bootstrap's scheme-less default port when the supervised client is built.
The current pooled constructor is plaintext-only, so it records 6650.
Moonpool has no supervised TLS constructor that creates this pool; adding one is outside this change.

`resolve_direct_broker` passes that default into `direct_broker_authority`.
The resulting physical address is always a validated `host:port` before the pool keys or dials it.
Proxy normalization and `strip_url_to_host_port` keep their existing stricter contracts and call the canonical function without a scheme-less fallback.

## Test Strategy

The first regression witness is a Moonpool DIRECT-routing integration test whose lookup advertises a portless host while a resolver is configured.
Before the fix it must fail because `split_host_port` receives no colon; after the fix it must resolve and dial port 6650.

The change carries the repository's cross-runtime layers:

- `magnetar-proto` unit tables cover explicit schemes, bare inputs with and without fallback, bracketed IPv6, invalid ports, empty authority, and malformed brackets.
- Tokio tests cover plaintext and TLS bootstrap fallbacks, explicit-scheme precedence, accurate rejection text, and agreement with the canonical normalizer.
- Moonpool integration tests cover the real resolver-to-dial path for a portless DIRECT target.
- Differential tests assert both engines select the same normalized physical authority and rejection shape.
- The existing real-Pulsar DIRECT-routing end-to-end test is rerun to prove full broker URLs and bootstrap reuse remain unchanged; Pulsar itself always advertises a full service URL, so it cannot emit the defensive portless wire shape.

The new Moonpool regression test must be observed red with production code unchanged, then green after the fix.
The production diff is searched against test literals after the green run so the fix cannot merely mirror a test string.

## Documentation and Compatibility

Add ADR-0091 to record the fallback contract, the newly discovered Moonpool behavior gap, and the choice to retain Tokio's `ParsedUrl` seam.
Update the ADR index and changelog in the same changeset.
Remove item 8 from `docs/follow-ups.md` only after the implementation and validation evidence exist.

The new proto helper is additive.
`probe_authority`, Tokio's `ParsedUrl`, and the façade API keep their signatures.
Earlier rejection of malformed authorities is a correctness hardening, not a façade break, and requires no `BREAKING CHANGE:` footer.

## Out of Scope

- Replacing Tokio's `ParsedUrl` with a proto-owned URL type.
- General DNS-label or internationalized-domain validation.
- Changing proxy forwarding behavior for broker URLs that Tokio currently warns about and forwards unchanged.
- Adding a Moonpool supervised TLS constructor or TLS-capable connection pool.

## Acceptance Criteria

- A portless DIRECT broker target reaches Tokio as `host:6650` on a plaintext bootstrap and `host:6651` on a TLS bootstrap.
- The same target reaches Moonpool as `host:6650` through its existing supervised plaintext pool.
- Explicit Pulsar schemes and ports override the bootstrap fallback.
- Invalid authorities fail before pool insertion or dialing.
- Tokio returns the same public `ParsedUrl` shape as before.
- The regression test is proven red and then green.
- Required formatter, build, lint, test, documentation, and xtask gates pass.
- Item 8 is absent from the follow-up tracker and ADR-0091 is accepted and indexed.
