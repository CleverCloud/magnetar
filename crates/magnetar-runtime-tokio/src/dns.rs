// SPDX-License-Identifier: Apache-2.0

//! Pluggable DNS resolver — Java parity for `ClientBuilder#dnsResolver`.
//!
//! Mirrors Netty's `io.netty.resolver.AddressResolver` shape: the runtime
//! hands the resolver a `(host, port)` pair, the resolver returns one or
//! more candidate [`SocketAddr`]s. Implementors can pin IPs, prefer
//! IPv4/IPv6, do split-horizon routing, plug a service-mesh sidecar
//! resolver, etc.
//!
//! Lives in `magnetar-runtime-tokio` (not `magnetar-proto`) because DNS is
//! by definition I/O — see [ADR-0004](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md).
//!
//! # Default
//!
//! When `ClientBuilder::dns_resolver(...)` is not called, the runtime falls
//! back to tokio's built-in DNS via [`TokioDnsResolver`], which wraps
//! [`tokio::net::lookup_host`]. That matches Java's behaviour when no
//! custom resolver is configured.
//!
//! # Why no `async-trait`
//!
//! `magnetar-runtime-tokio` deliberately avoids the `async-trait` crate to
//! keep the dep graph tight. The trait returns a boxed future explicitly
//! — slightly more verbose but stable Rust without macro magic.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::ClientError;

/// Boxed future returned by [`DnsResolver::resolve`].
pub type DnsResolveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<SocketAddr>, ClientError>> + Send + 'a>>;

/// Async DNS resolver — Java parity: `ClientBuilder#dnsResolver`.
///
/// Implementors are typically wrapped in an [`Arc`] and handed to
/// `magnetar::ClientBuilder::dns_resolver(...)`. The runtime polls the
/// resolver on every connection attempt (initial + reconnect).
///
/// The resolver returns a `Vec<SocketAddr>` rather than a single address so
/// callers can supply both A and AAAA results; the runtime picks the first
/// reachable candidate.
pub trait DnsResolver: Send + Sync + std::fmt::Debug {
    /// Resolve a `host:port` pair into one or more candidate
    /// [`SocketAddr`]s. The runtime tries each candidate in order until one
    /// connects.
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> DnsResolveFuture<'a>;
}

/// Default tokio-backed resolver. Wraps [`tokio::net::lookup_host`]. Used
/// when `ClientBuilder::dns_resolver(...)` is not called.
#[derive(Debug, Clone, Default)]
pub struct TokioDnsResolver;

impl DnsResolver for TokioDnsResolver {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> DnsResolveFuture<'a> {
        Box::pin(async move {
            let target = format!("{host}:{port}");
            let addrs = tokio::net::lookup_host(&target)
                .await
                .map_err(|e| ClientError::Other(format!("dns lookup_host({target}): {e}")))?;
            Ok(addrs.collect())
        })
    }
}

/// Convenience constructor — wrap any concrete resolver in an
/// [`Arc<dyn DnsResolver>`] ready to hand to `ClientBuilder`.
#[must_use]
pub fn arc_dns_resolver<R: DnsResolver + 'static>(resolver: R) -> Arc<dyn DnsResolver> {
    Arc::new(resolver)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[derive(Debug)]
    struct StaticIpResolver(SocketAddr);

    impl DnsResolver for StaticIpResolver {
        fn resolve<'a>(&'a self, _host: &'a str, _port: u16) -> DnsResolveFuture<'a> {
            let addr = self.0;
            Box::pin(async move { Ok(vec![addr]) })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn static_resolver_returns_its_address() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6650);
        let r = StaticIpResolver(addr);
        let out = r.resolve("ignored", 6650).await.expect("resolve ok");
        assert_eq!(out, vec![addr]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tokio_resolver_resolves_localhost() {
        let r = TokioDnsResolver;
        let out = r.resolve("localhost", 6650).await.expect("localhost ok");
        assert!(
            !out.is_empty(),
            "localhost should resolve to at least one address"
        );
        // Every address must point at the port we asked for.
        for addr in &out {
            assert_eq!(addr.port(), 6650);
        }
    }

    #[test]
    fn arc_wrapper_smoke() {
        let r = arc_dns_resolver(TokioDnsResolver);
        assert!(Arc::strong_count(&r) >= 1);
    }
}
