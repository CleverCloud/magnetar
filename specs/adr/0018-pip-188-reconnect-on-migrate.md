# ADR-0018 — PIP-188 `TOPIC_MIGRATED` → supervised reset + reconnect

- **Status**: Accepted
- **Date**: 2026-05-21
- **Decider**: Florentin Dubois
- **Tags**: pip-188, reconnect, supervisor, ha, java-parity

## Context

[PIP-188](https://github.com/apache/pulsar/wiki/PIP-188:-Topic-migration)
adds a broker-driven `CommandTopicMigrated` frame. When a topic moves
between clusters (e.g. for ops-driven rebalancing), the broker emits
`TOPIC_MIGRATED` with the new `brokerServiceUrl` /
`brokerServiceUrlTls`. The client is expected to:

1. Close its in-flight ops on the old broker.
2. Reconnect to the URL announced in the event.
3. Re-subscribe / re-produce as if it were a fresh client.

Magnetar already decoded the wire opcode (commit `7d568f9`) and surfaced
the `TopicMigrated` event from `Connection::poll_event`, but the
driver dispatched it as a logged-only no-op. The parity matrix listed
PIP-188 as `🟡`.

Constraints from prior ADRs:

- [ADR-0003 no-channels-rule](0003-no-channels-rule.md): no `oneshot`
  to signal "please rebuild now".
- The supervisor is already in place (commit `afda625`) and rebuilds
  producers + consumers (commit `cc465d9`). It triggers on
  `Connection::reset()` returning a `ClientError`.

## Decision

In `crates/magnetar-runtime-tokio/src/driver.rs`, the
`TopicMigrated { new_service_url }` event arm:

1. Logs the migration at `INFO` with the old + new URLs.
2. If the engine has a `ServiceUrlProvider` plumbed via
   [ADR-0016 PIP-121](0016-pip-121-cluster-failover.md), records the new
   URL into it (via downcast to `ControlledClusterFailover` or by
   replacing a static provider). This is opportunistic — the provider
   trait does not require a setter.
3. **Returns `ClientError::TopicMigrated(new_service_url)`** from the
   driver loop. The supervisor catches the error, calls
   `Connection::reset()`, applies the backoff schedule, and re-handshakes
   on the new URL (read from the provider on the next reconnect attempt).
4. The standard rebuild path (commit `cc465d9`) re-issues
   `CommandProducer` / `CommandSubscribe` on the new connection so
   in-flight producers and consumers transparently resume.

No new state machine in `magnetar-proto`. The wire event is already
there; the only behavioural change is the driver-loop arm returning
`ClientError` instead of swallowing the event.

## Consequences

- Topic migrations look identical to a transient disconnect to user
  code — the producer / consumer surface doesn't observe the
  reconnection.
- The `Backoff` struct (already in `magnetar-proto`) governs the
  reconnect cadence — same path as a network-level drop.
- A migration to a URL that fails to handshake degenerates to the same
  retry-and-give-up loop as any other reconnect. The supervisor's max
  attempts cap applies.
- This pairs cleanly with PIP-121: if the user has supplied an
  `AutoClusterFailover`, a `TOPIC_MIGRATED` to a known-failed URL would
  ride the failover policy on the next probe round.

## Redirect URL allow-list (2026-06-01)

### Threat model

PIP-188's `CommandTopicMigrated` carries a broker-supplied
`broker_service_url{,_tls}`. The supervised-reconnect path defined above
honours the hint and re-handshakes against the new URL using the **same**
[`AuthProvider`](../../crates/magnetar-proto/src/auth.rs) the original
connection was built with — token bearer, OAuth2 access token, SASL
PLAIN bytes, Kerberos GSSAPI, Athenz N-token. All of these providers
replay credential material on each `CommandConnect` via
`AuthProvider::initial()` (or on `CommandAuthChallenge` via
`AuthProvider::respond_to_challenge()`).

That gives a compromised broker — or a MITM downstream of TLS termination
at the broker side — a one-shot credential harvest: advertise an
attacker-controlled URL in `TopicMigrated`, wait for the client to
re-dial, capture the credential bytes the
[`auth_provider`](../../crates/magnetar-runtime-tokio/src/client.rs)
sends in the next `CommandConnect`. The same risk applies, with a wider
attack surface, to:

- `CommandLookupTopicResponse.broker_service_url{,_tls}` — the
  proxy-mode lookup answer that the runtime injects into
  `CommandConnect.proxy_to_broker_url` on the next handshake against the
  pool entry (ADR-0039).
- `CommandCloseProducer.assigned_broker_service_url{,_tls}` /
  `CommandCloseConsumer.assigned_broker_service_url{,_tls}` — broker
  hints emitted alongside fenced producers/consumers.

Today the magnetar runtime only **logs** these URLs and reconnects to
the cached service URL, so the immediate exposure is partial. But the
hint hooks are documented as "opportunistic" in the original ADR; any
future plumbing that lets the URL actually drive the re-dial inherits
the credential-harvest risk. Defence in depth is cheaper to land before
the URL is wired through than after.

### Decision

Add an opt-in allow-list on
[`ConnectionConfig`](../../crates/magnetar-proto/src/conn_types.rs):

```rust
pub struct ConnectionConfig {
    // ... existing fields ...
    pub redirect_url_allow_list: Option<RedirectUrlAllowList>,
}

pub enum RedirectUrlAllowList {
    /// Accept URLs whose host literal (lowercased) is in the set.
    Hosts(Vec<String>),
    /// Accept URLs whose full URL string matches verbatim.
    Exact(Vec<String>),
}
```

`Some(_)` enables the gate; `None` is the **default**, preserving
pre-allow-list behaviour. The proto state machine validates every
broker-advertised URL in `CommandTopicMigrated` (and is shaped to extend
to the lookup + close commands as follow-ups) **before** surfacing the
event that drives the runtime's reconnect. Rejected URLs surface
[`ConnectionEvent::RedirectUrlRejected`](../../crates/magnetar-proto/src/event.rs)
instead. The runtime engines log the rejection at `warn!` and **do not**
return an error from the driver loop — the supervised reconnect arm
stays asleep, the original `AuthProvider::initial()` credentials are not
handed to the unverified host, and the existing channel keeps serving
until the broker tears it down on its own.

### Why `None` by default

Two reasons:

1. **Source-compatibility for callers.** ADR-0018 shipped without the
   gate; existing builder code, builder examples, the parity matrix's
   PIP-188 row, and the e2e suite all assume the URL is followed.
   Defaulting to `Some([broker.service.url])` would force every caller
   to opt in or break.
2. **The right default depends on deployment.** A single-tenant
   deployment behind a known set of brokers wants
   `Hosts(["broker-a.example.com", "broker-b.example.com", ...])`. A
   PIP-121 cluster-failover deployment with an external URL source wants
   `Exact(<full set>)`. A development cluster wants `None`. Magnetar
   can't pick for the operator — the most we can do is make the gate
   cheap, well-documented, and visible in the ADR.

### Tests

Per ADR-0024 cross-runtime test + coverage policy:

1. **`magnetar-proto` unit tests** —
   `crates/magnetar-proto/src/conn_types.rs::redirect_url_allow_list_tests`:
   `Default` is `None`, `Hosts` matches case-insensitively, IPv6
   bracketed authorities work, non-Pulsar schemes are rejected,
   unparseable URLs are rejected, empty lists reject everything.
2. **`magnetar-runtime-tokio` integration** —
   `crates/magnetar-runtime-tokio/tests/topic_migrated_allow_list.rs`:
   broker stub pushes a disallowed migration URL after producer-open;
   the test asserts that **no second `CommandConnect`** lands at the
   stub (no auth replay) and that the default-permissive mode still
   triggers the reconnect.
3. **`magnetar-runtime-moonpool` integration** —
   `crates/magnetar-runtime-moonpool/tests/topic_migrated_allow_list.rs`:
   synthetic frame injection mirrors the tokio path. Asserts
   `RedirectUrlRejected` surfaces and `is_connected()` stays `true`
   after rejection.
4. **`magnetar-differential` equivalence** —
   `crates/magnetar-differential/tests/redirect_url_allow_list_equivalence.rs`:
   both engines must surface the same `RedirectUrlRejected`
   step-by-step under the same wire input.
5. **e2e** — no new e2e test. The default `None` makes the existing e2e
   suite a regression test for the un-configured path; an e2e test with
   a real broker that _enforces_ the allow-list would require staging a
   malicious URL on the broker side, which `apachepulsar/pulsar:4.0.4`
   does not support out of the box.

## References

- `crates/magnetar-runtime-tokio/src/driver.rs` — `TopicMigrated` arm
  in the event loop
- `crates/magnetar-proto/src/conn.rs` — `Connection::reset()` (Stage 2
  supervisor primitive)
- `crates/magnetar-proto/src/conn_types.rs` — `RedirectUrlAllowList` +
  threat-model rustdoc
- `crates/magnetar-proto/src/event.rs` — `ConnectionEvent::RedirectUrlRejected`
- Commit `7d568f9` — "feat(proto): PIP-188 TopicMigrated event surfaced from CommandTopicMigrated"
- Commit `9a35db4` — "feat(pip-188): handle TopicMigrated by triggering supervised reset+reconnect"
- Commit `afda625` — "feat(supervisor): wire reconnect into tokio driver_loop (Stage 2)"
- Commit `cc465d9` — "feat(supervisor): rebuild producers + consumers across reconnect (Stage 3)"
- Apache PIP-188
- [ADR-0016 pip-121-cluster-failover](0016-pip-121-cluster-failover.md)
- [ADR-0024 cross-runtime test + coverage policy](0024-cross-runtime-test-and-coverage-policy.md)
- Lookup multi-agent review MEDIUM-1 (`~/.claude/state/zk-lookup-review-report.md`)
