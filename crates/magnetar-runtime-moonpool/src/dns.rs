// SPDX-License-Identifier: Apache-2.0

//! Pluggable DNS resolver — moonpool engine flavour.
//!
//! Mirrors the `magnetar_runtime_tokio::dns::DnsResolver` trait but lives
//! behind the `magnetar-runtime-moonpool` `Providers`-generic transport so
//! the deterministic-simulation substrate can plug in a virtual resolver.
//! Java parity: `ClientBuilder#dnsResolver`. Lives in the runtime crate (not
//! `magnetar-proto`) because DNS is, by definition, I/O — see
//! [ADR-0004](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md).
//!
//! # Default
//!
//! When no resolver is configured on the internal `crate::transport::Transport`
//! connect path, the engine falls back to whatever the configured
//! [`moonpool_core::NetworkProvider::connect`] does with a `host:port`
//! string — which is `tokio::net::TcpStream::connect` for
//! `TokioNetworkProvider` and the in-memory virtual fabric for a moonpool
//! simulation provider. Matches Java's "no custom resolver" behaviour.
//!
//! # Why no `async-trait`
//!
//! Same reasoning as the tokio crate: keep the dep graph tight. The trait
//! returns a boxed future explicitly so it stays object-safe without macro
//! magic. See [ADR-0015](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0015-dns-resolver-injection.md).
//!
//! # Determinism
//!
//! The resolver is consulted **before** the internal `crate::transport::Transport`
//! hands the `(host, port)` pair to the moonpool [`moonpool_core::NetworkProvider`].
//! Inside `moonpool-sim`, that lets the resolver hand back a virtual
//! `127.0.x.y:port` address that the simulator's network fabric routes
//! deterministically. The [`StaticDnsResolver`] helper below is the
//! canonical "pin one IP for every host" implementation; sim tests typically
//! use it directly.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use crate::EngineError;

/// Boxed future returned by [`DnsResolver::resolve`]. The lifetime is
/// `'a` so resolvers can borrow from `self` (e.g. a pre-baked
/// `Vec<SocketAddr>`) without an extra allocation.
pub type DnsResolveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<SocketAddr>, EngineError>> + Send + 'a>>;

/// Async DNS resolver — moonpool flavour. Mirrors the tokio engine's
/// `magnetar_runtime_tokio::dns::DnsResolver` but returns
/// [`EngineError`] so callers can `?` straight from the moonpool transport
/// layer.
///
/// The resolver returns a `Vec<SocketAddr>` rather than a single address so
/// callers can supply both A and AAAA results; the transport picks the
/// first candidate that connects.
pub trait DnsResolver: Send + Sync + std::fmt::Debug {
    /// Resolve a `host:port` pair into one or more candidate
    /// [`SocketAddr`]s. The transport tries each candidate in order until
    /// one connects.
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> DnsResolveFuture<'a>;
}

/// Deterministic DNS resolver — pins every lookup to one or more fixed
/// [`SocketAddr`]s.
///
/// The canonical resolver for `moonpool-sim` tests: pair it with the
/// simulator's virtual network fabric so the broker / proxy / lookup
/// targets all resolve to known virtual addresses. The host argument is
/// **ignored**; if you need a multi-host fixture, wrap several
/// `StaticDnsResolver`s in a routing layer.
#[derive(Debug, Clone)]
pub struct StaticDnsResolver {
    addrs: Vec<SocketAddr>,
}

impl StaticDnsResolver {
    /// Build a resolver that always returns `addrs` (in order) regardless
    /// of the requested host / port.
    #[must_use]
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        Self { addrs }
    }

    /// Convenience: single-address constructor.
    #[must_use]
    pub fn single(addr: SocketAddr) -> Self {
        Self { addrs: vec![addr] }
    }
}

impl DnsResolver for StaticDnsResolver {
    fn resolve<'a>(&'a self, _host: &'a str, _port: u16) -> DnsResolveFuture<'a> {
        let addrs = self.addrs.clone();
        Box::pin(async move { Ok(addrs) })
    }
}

/// Convenience constructor — wrap any concrete resolver in an
/// [`Arc<dyn DnsResolver>`] ready to hand to the engine.
#[must_use]
pub fn arc_dns_resolver<R: DnsResolver + 'static>(resolver: R) -> Arc<dyn DnsResolver> {
    Arc::new(resolver)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn static_resolver_returns_its_address() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6650);
        let r = StaticDnsResolver::single(addr);
        let out = r.resolve("ignored", 6650).await.expect("resolve ok");
        assert_eq!(out, vec![addr]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn static_resolver_returns_all_addrs_in_order() {
        let a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6650);
        let b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 6650);
        let r = StaticDnsResolver::new(vec![a, b]);
        let out = r.resolve("ignored", 6650).await.expect("resolve ok");
        assert_eq!(out, vec![a, b]);
    }

    #[test]
    fn arc_wrapper_smoke() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6650);
        let r = arc_dns_resolver(StaticDnsResolver::single(addr));
        assert!(Arc::strong_count(&r) >= 1);
    }
}
