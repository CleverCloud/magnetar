# ADR-0087 — Unify the broker-URL authority parsers on `probe_authority`

- **Status**: Accepted (amends [ADR-0085](0085-probe-endpoint-parsing-in-proto.md))
- **Date**: 2026-07-30
- **Decider**: Florentin Dubois
- **Tags**: sans-io, health-probe, lookup, routing, ipv6, bit-flip-survivability, robustness, drift-prevention

## Context

[ADR-0085](0085-probe-endpoint-parsing-in-proto.md) single-sourced the **health-probe** endpoint parse into `magnetar_proto::probe_authority` after discovering that both engines' `authority()` were byte-identical copies of one parser and had rotted identically — `strip_prefix(…).unwrap_or(endpoint)` fell through on an unrecognised scheme and the following `split('/')` truncated a bit-flipped `"ptlsar://broker:6650"` into the nonsense authority `"ptlsar:"`, which each engine then dialled.

It deliberately stopped there, filing the sibling parsers as the "Broker-URL authority parser unification" entry in `docs/follow-ups.md` (§8).
Its own "Scope boundary" section named the reason: those parsers return `Result<String, ClientError>` with caller-specific error text and sit on the lookup/routing path, whose blast radius (a corrupted value reaching `CommandConnect.proxy_to_broker_url` on the wire, issue #364) differs from a probe verdict.

That left **four** hand-rolled copies of the same "strip `pulsar://` / `pulsar+ssl://`, trim the path, synthesise the scheme default port" table:

| Site                      | File                                             | Contract                                                            |
| ------------------------- | ------------------------------------------------ | ------------------------------------------------------------------- |
| `probe_authority`         | `crates/magnetar-proto/src/health_probe.rs`      | canonical; scheme optional, bare `host:port` accepted               |
| `proxy_broker_authority`  | `crates/magnetar-runtime-moonpool/src/client.rs` | same rules, `Result<String, ClientError>`                           |
| `direct_broker_authority` | `crates/magnetar-runtime-moonpool/src/client.rs` | delegates to `proxy_broker_authority`, distinct contract docstring  |
| `strip_url_to_host_port`  | `crates/magnetar-runtime-moonpool/src/driver.rs` | scheme **mandatory** (service URLs must carry one); trims `?` / `#` |

They agreed only because each had been written to match — the arrangement that produced the ADR-0085 defect in the first place.

Two concrete defects follow from the duplication rather than from any one copy:

- **Port-less bracketed IPv6.** Default-port synthesis triggered on `!authority.contains(':')`, which is never true of a bracketed IPv6 literal whose colons belong to the address. So `pulsar://[::1]` yielded the port-less `"[::1]"` and every dialer rejected it. ADR-0085 recorded this as its one accepted limitation, inherited from `proxy_broker_authority` precisely so the two could not diverge; `strip_url_to_host_port` shared it independently. Fixing it in one copy without the others is the drift §8 exists to prevent.
- **No empty-authority check in `proxy_broker_authority`.** `probe_authority` rejects `""` and `"pulsar://"` — the latter check ordered deliberately _before_ synthesis so it cannot produce the garbage authority `":6650"`. The moonpool copy had neither check, so `""` returned `Ok("")` and `"pulsar://"` returned `Ok(":6650")`, and those values went on to the wire in `CommandConnect.proxy_to_broker_url`. This is the same class of defect as issue #364 — a fabricated authority reaching the proxy — and it survived that fix because the reference sweep looked for scheme truncation, not for an absent emptiness guard.

A fifth site, `magnetar_runtime_tokio::client::parse_direct_broker_url`, applies the same rule but parses via the `url` crate into a `ParsedUrl { host, port }` struct rather than a `host:port` string.
§8 marked it "maybe".

## Decision

The scheme / default-port rule has exactly **one** implementation: `magnetar_proto::probe_authority`.

- **Close the IPv6 gap in the canonical parser.** A new private `authority_has_explicit_port` answers "does this authority already carry a `:port`?" correctly: for a bracketed authority the port, when present, follows the closing `]`. An unterminated bracket (`[::1`) is malformed and gets no synthesised port — appending one to a string we cannot parse would only fabricate different garbage, and returning it verbatim is what the function did before brackets were handled at all.
- **`proxy_broker_authority` becomes a delegation** that maps `None` to `ClientError::Other`. `direct_broker_authority` keeps its own contract docstring and its one-line body over `proxy_broker_authority`, unchanged.
- **`strip_url_to_host_port` gates, then delegates.** Its stricter contract is enforced locally — the scheme is mandatory, so a bare `host:port` still returns `None` — and the `?` / `#` trim stays local too, because only service URLs carry a query or fragment. Only then is the scheme-strip + default-port decision delegated. Its previous body conceded in a comment that it could not "tell the schemes apart cheaply" and re-derived the default port from a second `starts_with` pass; delegation makes that free.
- **One rejection message, reworded.** `probe_authority` folds unrecognised-scheme, empty-input and scheme-with-no-authority into a single `None`, so the caller has one message to give. The text it replaces asserted "carries an unrecognised scheme", which is false for `""` — a message that would have been actively misleading on the newly-rejected inputs. The new text names the three accepted shapes instead. No test asserted on the old string (every site matches only the `ClientError::Other` variant, including the differential suite's `RejectionShape` classifier), so the rewording is contained; it is recorded in `CHANGELOG.md` because it is user-visible.
- **`parse_direct_broker_url` stays on `url`.** Folding it in would mean either giving up the `ParsedUrl` struct return or wrapping `probe_authority` and re-splitting its output — trading a real seam for a cosmetic one. Instead it gains a table-driven **equivalence audit** (`parse_direct_broker_url_agrees_with_probe_authority`) asserting row by row where it agrees with `probe_authority` and where it deliberately does not. Unified-by-construction is unavailable here, so the next best thing is that a divergence cannot change without someone editing a table that says why.

### The two deliberate divergences, now pinned

Discovered by running the audit rather than by reading the code, and recorded so neither reads as an accident:

- **Scheme-less input.** `probe_authority("b-c3-n12")` returns `"b-c3-n12"` — there is no scheme to take a default port from. `parse_direct_broker_url` synthesises a URL from the _bootstrap_ scheme and so always lands a port (`"b-c3-n12:6650"`). Same for `[::1]`. The engines differ in representation here, not in what they accept, which the pre-existing `*_accepts_bare_host_port` tests on both sides already described.
- **Malformed bracket.** `url` rejects `pulsar://[::1` outright (`invalid IPv6 address`); `probe_authority` returns it verbatim. Both refuse to fabricate a port; they differ only in whether the caller or the dialer says no.

### Behaviour changes

| Input                                 | Before            | After                        |
| ------------------------------------- | ----------------- | ---------------------------- |
| `pulsar://[::1]`                      | `"[::1]"`         | `"[::1]:6650"`               |
| `pulsar+ssl://[2001:db8::1]`          | `"[2001:db8::1]"` | `"[2001:db8::1]:6651"`       |
| `proxy_broker_authority("")`          | `Ok("")`          | `Err(ClientError::Other(_))` |
| `proxy_broker_authority("pulsar://")` | `Ok(":6650")`     | `Err(ClientError::Other(_))` |

`pulsar://[::1]:6650`, bare `host:port`, scheme-less hosts, trailing-path trimming and the unrecognised-scheme rejection are all unaffected.
The first two rows change the observable output of the public `magnetar_proto::probe_authority`; no signature changes, so there is no `BREAKING CHANGE:` footer.

## Consequences

**Positive**

- The rule has one implementation and four call sites. A future engine cannot re-fork it silently, and `broker_authority_parsers_agree_with_probe_authority` goes red on any row that diverges — verified by reverting the delegation and watching it fail.
- Port-less bracketed IPv6 service URLs and broker-advertised URLs now dial correctly on every path at once, which is the property ADR-0085 could not offer for a one-sided fix.
- An empty or scheme-only broker-advertised URL can no longer reach `CommandConnect.proxy_to_broker_url`.
- `strip_url_to_host_port` keeps its stricter contract while losing its copy of the shared rule — the scheme-gate-then-delegate seam generalises to any future caller that needs different strictness over the same table.

**Negative**

- `magnetar-proto` is now on the lookup/routing path as well as the probe path, so a regression in `probe_authority` has a wider blast radius than ADR-0085 left it with. That is the trade the unification buys: one place to get wrong instead of four, at the cost of that place mattering more. The four-layer test set plus the equivalence audit is what makes it acceptable.
- The moonpool rejection message changed. Anything grepping logs for "carries an unrecognised scheme" against the moonpool engine needs updating (the tokio engine's own message is untouched).

**Neutral**

- `parse_direct_broker_url` remains a fifth implementation, now audited rather than unified. Its tokio-side message has the same imprecision the moonpool one had (it says "unrecognised scheme" for `pulsar://`, whose real fault is the missing authority); left alone deliberately, since changing it is a separate user-visible text change with no correctness content. Recorded as the residual in `docs/follow-ups.md` §8.

### Alternatives considered

- **Thread a proto-level error type into `ClientError`** (e.g. `probe_authority` returning `Result<String, AuthorityRejection>` so the caller can phrase one message per rejection class). Rejected: it grows `magnetar-proto`'s public API with a compatibility commitment in order to distinguish cases whose only difference is prose, and `docs/follow-ups.md` §8 had already framed the caller-maps-`None` option as the lighter of the two. The cost is one slightly broader message, which the rewording absorbs.
- **Keep the old error text verbatim and map every `None` to it.** Rejected: it would tell an operator that `""` "carries an unrecognised scheme", i.e. lie about the newly-rejected inputs, on the routing path where the message is the only diagnostic.
- **Leave `strip_url_to_host_port` alone**, as §8's inventory suggested ("stricter on purpose... already correct"). Rejected on re-reading: "already correct" referred to the ADR-0085 scheme defect, which it never had. It did share the IPv6 gap, so leaving it would have closed the gap in three parsers and left the fourth behind — reconstructing exactly the silent divergence this entry was filed to prevent.
- **Also fold in `parse_direct_broker_url`.** Rejected for now: see the Decision. A struct return over a string parse is a genuinely different seam, and the audit test covers the drift risk without a contorted adapter.

## References

- `docs/follow-ups.md` §8 — the entry this closes, trimmed to the `parse_direct_broker_url` residual.
- `crates/magnetar-proto/src/health_probe.rs` — `probe_authority`, `authority_has_explicit_port`, ADR-0024 layer (a).
- `crates/magnetar-runtime-moonpool/src/client.rs` — the delegating `proxy_broker_authority` / `direct_broker_authority` plus the table-driven equivalence test.
- `crates/magnetar-runtime-moonpool/src/driver.rs` — `strip_url_to_host_port`'s scheme gate + delegation.
- `crates/magnetar-runtime-tokio/src/client.rs` — `parse_direct_broker_url`'s equivalence audit.
- `crates/magnetar-runtime-{tokio,moonpool}/tests/probe_portless_ipv6.rs` — layers (b) / (c): the tracing witness that the synthesised port is what gets dialled.
- `crates/magnetar-differential/tests/broker_authority_ipv6_equivalence.rs` — layer (d): both engines agree on the port-less IPv6 authority, and both still refuse a corrupted scheme.
- `crates/magnetar/tests/e2e_probe_portless_ipv6.rs` — end-to-end: a port-less IPv6 primary is probed healthy against a real listener, then fails over to a real broker.
- [ADR-0085](0085-probe-endpoint-parsing-in-proto.md) — amended here: its accepted IPv6 limitation is closed and its scope boundary is lifted.
- [ADR-0039](0039-pulsar-proxy-multi-broker-connection-model.md), [ADR-0045](0045-proxy-to-broker-url-host-port-format.md) — the `proxy_to_broker_url` wire contract the moonpool parsers serve.
- [ADR-0055](0055-bit-flip-survivability-model.md) — the reference-sweep discipline that surfaced the original defect.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the test layers shipped with this.
