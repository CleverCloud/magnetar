# ADR-0015 — DNS resolver trait + `TokioDnsResolver`

- **Status**: Accepted
- **Date**: 2026-05-21
- **Decider**: Florentin Dubois
- **Tags**: dns, networking, testability, java-parity

## Context

Apache Pulsar's Java client lets users plug in a custom DNS resolver via `ClientBuilder.dnsResolver(InetAddressResolver)`.
Use cases:

- Service discovery via Consul / SkyDNS / cluster-local DNS.
- Round-robin pinning to a known set of brokers in tests.
- Latency-aware resolution (prefer "near" broker).

Magnetar's `Transport::connect` historically hard-coded `tokio::net::TcpStream::connect(&service_url)`, which forwards to the OS resolver.
There was no override seam.

[ADR-0004 sans-io-protocol-core](0004-sans-io-protocol-core.md) forbids I/O in `magnetar-proto`, so any resolver trait must live in the engine crate, not the protocol crate.

## Decision

Add a `DnsResolver` trait in `magnetar-runtime-tokio`:

```rust
pub trait DnsResolver: Send + Sync + 'static {
    fn resolve(
        &self,
        host: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send + '_>>;
}
```

- **Boxed-future return** (no `async-trait` crate dependency — banned by the same lean-dep posture that drove ADR-0003).
- Vec<SocketAddr> covers multi-A/AAAA responses.
- The trait sits next to `Transport` in `magnetar-runtime-tokio`.

Default implementation: `TokioDnsResolver` calls `tokio::net::lookup_host(format!("{host}:0"))` and collects the addrs.

`Transport::connect_with_resolver(addr, resolver: Arc<dyn DnsResolver>)` parses the URL host:port, calls `resolver.resolve(host)`, then connects to the first address that succeeds.

`ClientBuilder::dns_resolver(resolver: Arc<dyn DnsResolver>)` plumbs the override down to `ConnectionShared` → `Transport::connect_with_resolver`.

## Consequences

- Custom resolvers live in user code; magnetar takes a trait object.
- Tests can swap a fake resolver that returns a static `Vec` without spinning up a DNS server.
- The default path is `TokioDnsResolver` — production code keeps the ergonomics of the OS resolver.
- `magnetar-proto` is untouched; the trait + plumbing all live in `magnetar-runtime-tokio`.
  Moonpool engine will pick up its own in-memory resolver when M5 (full moonpool engine) lands.
- Boxed-future return is an extra heap allocation per lookup.
  Acceptable given DNS is rare (once per reconnect cycle) and the alternative is pulling `async-trait` into the dep graph.

## References

- `crates/magnetar-runtime-tokio/src/dns.rs` — `DnsResolver` trait + `TokioDnsResolver`
- `crates/magnetar-runtime-tokio/src/transport.rs` — `connect_with_resolver`
- `crates/magnetar/src/client.rs` — `ClientBuilder::dns_resolver`
- Commit `bada674` — "feat(client): DNS resolver skeleton + parity-matrix catch-up"
- Commit `61e2d7c` — "feat(client): wire DnsResolver through Transport::connect"
- Java reference: `org.apache.pulsar.client.api.ClientBuilder#dnsResolver`
- [ADR-0003 no-channels-rule](0003-no-channels-rule.md)
- [ADR-0004 sans-io-protocol-core](0004-sans-io-protocol-core.md)
