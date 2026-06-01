# ADR-0045 — `CommandConnect.proxy_to_broker_url` is `host:port`, not a `pulsar://` URL

- **Status**: Accepted (amends [ADR-0039](0039-pulsar-proxy-multi-broker-connection-model.md))
- **Date**: 2026-06-01
- **Decider**: Florentin Dubois
- **Tags**: protocol, proxy, wire-format, ADR-0039-amendment

## Context

[ADR-0039](0039-pulsar-proxy-multi-broker-connection-model.md) landed the
per-broker pool model for talking to the Apache Pulsar Proxy: when
`CommandLookupTopic` answers `proxy_through_service_url = true`, the
runtime opens a second TCP connection back to the proxy with
`CommandConnect.proxy_to_broker_url = <broker_url>` set to the value the
broker advertised in `broker_service_url(_tls)`. ADR-0039 §"Incompatibilities"
asserted there was nothing more to do on the wire because magnetar already
encodes the field.

That claim was wrong. ADR-0039 left the *format* of the field implicit, and
magnetar piped the broker's advertised value (e.g.
`pulsar://clevercloud-pulsar-broker-c3-n12:6650`) into `CommandConnect`
verbatim. The Apache Pulsar Proxy parses this field via
`InetSocketAddress.createUnresolved(host, port)` after splitting on `:`; with
an unstripped scheme it sees `host = "pulsar"`, `port = "//host:6650"`,
`validateBrokerTarget()` returns `false`, and the handshake is rejected
with `ServerError.ServiceNotReady "Target broker cannot be validated"`.

Observed against `pulsar+ssl://materiamq.eu-fr-1.services.clever-cloud.com:6651`
(Clever Cloud managed Pulsar 4.x):

```
WARN magnetar_proto::conn: captured CommandError during handshake state=ConnectSent
     server_error=ServiceNotReady Target broker cannot be validated.
ERROR magnetar: engine error: open_producer: other: handshake failed:
     broker rejected handshake (server_error=ServiceNotReady):
     Target broker cannot be validated.
```

Reference clients send the value scheme-less:

- **Java** — `ClientCnx#channelActive` builds the target via
  `String.format("%s:%d", logicalAddress.getHostString(), logicalAddress.getPort())`
  before passing it to `Commands.newConnect(..., targetBroker)`.
- **pulsar-rs** — `service_discovery.rs:135` builds
  `broker_url = format!("{}:{}", u.host_str().unwrap(), u.port().unwrap_or(broker_port))`
  with `broker_port = 6650` for `pulsar` and `6651` for `pulsar+ssl`; this
  is the value `connection_manager.rs:368` stuffs into
  `CommandConnect.proxy_to_broker_url`.

## Decision

`CommandConnect.proxy_to_broker_url` MUST carry the broker authority as
`host:port` — no `pulsar://` or `pulsar+ssl://` scheme prefix and no path.

Implementation:

- `magnetar_runtime_tokio::client::preferred_broker_url` parses the
  advertised string via `ParsedUrl::parse` and returns
  `format!("{host}:{port}")`. The default port comes from the *URL's*
  scheme (6650 for `pulsar`, 6651 for `pulsar+ssl`) — same convention as
  pulsar-rs and Java.
- `magnetar_runtime_moonpool::client::proxy_broker_authority` mirrors the
  scheme-strip step using a string-based helper (the moonpool crate does
  not depend on `url`). Its output flows into `LookupTarget::Proxy { broker_url }`
  so when moonpool proxy routing lands as a follow-up it produces the same
  wire bytes as the tokio engine.
- Inputs the helpers cannot parse fall back to forwarding the string
  unchanged with a `tracing::warn!`. The lookup contract says this
  shouldn't happen; if it does, the warn lets us tell "broker advertised
  garbage" apart from "we forgot to strip".

The bootstrap connection still sends `CommandConnect.proxy_to_broker_url
= None`. Only the per-broker pool entries set the field, and they set it
to `host:port`.

## Consequences

**Easier**:
- Magnetar handshakes succeed against the Apache Pulsar Proxy in the
  proxy-through-service-url path. The fallback to `pulsar-rs` for
  Clever Cloud's materiamq path is no longer needed once this lands.
- ADR-0039's wire-format claim is now grounded in tested code rather
  than left implicit. The new unit tests in both runtime crates pin the
  helpers, so a future refactor that drops the strip cannot regress
  silently.

**Harder**:
- One extra parse per pool-entry open. The cost is negligible compared
  with the TLS handshake on the same path.

**Incompatibilities**:
- Wire-visible change for proxied producers/consumers — previous magnetar
  releases sent `pulsar://host:port` on the second connection and were
  silently rejected by real Pulsar proxies. There is no production
  workload that benefited from the old behaviour.

## Tests (ADR-0024 four-layer matrix)

- **`magnetar-proto`** — no change. The proto layer faithfully encodes
  whatever `ConnectionConfig.proxy_to_broker_url` carries; the existing
  test `lookup.rs::connect_outcome_honours_proxy_through_service_url`
  continues to cover the lookup side.
- **`magnetar-runtime-tokio`** — `preferred_broker_url` gets six unit
  tests covering: scheme strip on `pulsar+ssl://` and `pulsar://`,
  fallback when only one of the two URLs is advertised, default-port
  inference, `None` propagation, and unparseable-input passthrough. The
  existing `tests/proxy_multi_conn.rs` integration test is updated to
  assert the per-broker `CommandConnect` carries the `host:port` form
  (`ADVERTISED_BROKER_HOST_PORT`).
- **`magnetar-runtime-moonpool`** — `proxy_broker_authority` gets six
  unit tests mirroring the tokio side. The
  `crate::client::lookup_topic_target` call path threads the helper
  output into `LookupTarget::Proxy { broker_url }` so the wire format
  is consistent when moonpool proxy routing lands.
- **`magnetar-differential`** — no change; proxy routing isn't part of
  the current differential workload. Tracked alongside the moonpool
  proxy routing follow-up.
- **`crates/magnetar/tests/e2e_*.rs`** — covered by the existing
  proxy-aware e2e workloads gated behind `--features e2e` once
  Docker compose with `apachepulsar/pulsar:4.0.4` is up.

## References

- `crates/magnetar-runtime-tokio/src/client.rs` — `preferred_broker_url`
  and its unit tests.
- `crates/magnetar-runtime-moonpool/src/client.rs` — `proxy_broker_authority`
  and its unit tests.
- `crates/magnetar-runtime-tokio/tests/proxy_multi_conn.rs` — integration
  test asserting the `host:port` wire format end-to-end.
- Upstream:
  [`ClientCnx.java`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ClientCnx.java)
  (`channelActive` → `Commands.newConnect(..., targetBroker)`),
  [`ProxyConnection.java`](https://github.com/apache/pulsar/blob/master/pulsar-proxy/src/main/java/org/apache/pulsar/proxy/server/ProxyConnection.java)
  (`handleConnect` → `validateBrokerTarget` → "Target broker cannot be validated").
- pulsar-rs:
  [`service_discovery.rs`](https://github.com/streamnative/pulsar-rs/blob/master/src/service_discovery.rs)
  (`lookup_topic` → `broker_url = format!("{}:{}", ...)`).
- Related ADRs: [ADR-0039](0039-pulsar-proxy-multi-broker-connection-model.md)
  (amended by this one), [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md)
  (test matrix).
