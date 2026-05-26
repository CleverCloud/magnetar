// SPDX-License-Identifier: Apache-2.0

//! TCP / TLS transport layer for the tokio engine.
//!
//! Wraps either a plain [`tokio::net::TcpStream`] or a `tokio_rustls::client::TlsStream<TcpStream>`
//! behind a single enum so the driver loop can stay generic over the two. We deliberately keep
//! this very thin — Pulsar's binary protocol does not multiplex TLS records, so a single
//! `AsyncRead + AsyncWrite + Unpin` is all the driver needs.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::ServerName;

use crate::dns::DnsResolver;
use crate::error::ClientError;
use crate::url_parse::{ParsedUrl, Scheme};

/// Either a plaintext TCP stream or a TLS-wrapped TCP stream.
///
/// The engine drives this through the `AsyncRead + AsyncWrite` interface only — TLS-specific
/// state lives entirely inside `tokio-rustls`.
#[derive(Debug)]
pub(crate) enum Transport {
    /// Plaintext `pulsar://` connection.
    Plain(TcpStream),
    /// TLS `pulsar+ssl://` connection.
    Tls(Box<TlsStream<TcpStream>>),
}

impl Transport {
    /// Connect to `url`, optionally upgrading to TLS using `tls_config` for `pulsar+ssl://`.
    ///
    /// `tls_config` is required when `url.scheme == Scheme::Tls`. For plaintext connections it is
    /// ignored. DNS resolution falls back to tokio's built-in [`tokio::net::lookup_host`] —
    /// see [`Self::connect_with_resolver`] for the resolver-aware flavour.
    ///
    /// Kept as a thin wrapper over [`Self::connect_with_resolver`] for callers that don't need
    /// the pluggable DNS resolver hook (tests, future generic-socket flavours).
    #[allow(dead_code)]
    pub(crate) async fn connect(
        url: &ParsedUrl,
        tls_config: Option<Arc<rustls::ClientConfig>>,
    ) -> Result<Self, ClientError> {
        Self::connect_with_resolver(url, tls_config, None).await
    }

    /// Connect to `url`, routing DNS resolution through `resolver` when `Some`.
    ///
    /// When `resolver = Some`, the resolver returns one or more [`std::net::SocketAddr`]
    /// candidates and we dial each in order, returning the first that connects. If every
    /// candidate fails, the last [`std::io::Error`] is surfaced. When `resolver = None`
    /// we fall back to tokio's built-in [`tokio::net::TcpStream::connect`] over the
    /// `(host, port)` tuple — identical to [`Self::connect`].
    ///
    /// `tls_config` semantics match [`Self::connect`]. The TLS server-name still comes
    /// from `url.host`, not from the resolved address, so SNI / hostname verification
    /// stay correct even when the resolver pins a specific IP.
    pub(crate) async fn connect_with_resolver(
        url: &ParsedUrl,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        resolver: Option<&dyn DnsResolver>,
    ) -> Result<Self, ClientError> {
        let tcp = match resolver {
            Some(r) => {
                let addrs = r.resolve(&url.host, url.port).await?;
                if addrs.is_empty() {
                    return Err(ClientError::Other(format!(
                        "dns resolver returned no addresses for {}:{}",
                        url.host, url.port
                    )));
                }
                let mut last_err: Option<std::io::Error> = None;
                let mut connected: Option<TcpStream> = None;
                for addr in addrs {
                    match TcpStream::connect(addr).await {
                        Ok(s) => {
                            connected = Some(s);
                            break;
                        }
                        Err(e) => {
                            last_err = Some(e);
                        }
                    }
                }
                match connected {
                    Some(s) => s,
                    None => {
                        // Unwrap is safe: `addrs` was non-empty, so either we connected or
                        // we recorded at least one error.
                        return Err(last_err
                            .expect("at least one connect attempt was made")
                            .into());
                    }
                }
            }
            None => TcpStream::connect(url.socket_addr()).await?,
        };
        // Pulsar broker performance presumes Nagle is off; matches the Java client.
        let _ = tcp.set_nodelay(true);

        match url.scheme {
            Scheme::Plain => Ok(Self::Plain(tcp)),
            Scheme::Tls => {
                let config = tls_config.ok_or_else(|| {
                    ClientError::Other(
                        "pulsar+ssl:// requires a rustls::ClientConfig; pass Some(_)".to_owned(),
                    )
                })?;
                let server_name = ServerName::try_from(url.host.clone())
                    .map_err(|_| ClientError::InvalidServerName(url.host.clone()))?;
                let connector = TlsConnector::from(config);
                let tls = connector.connect(server_name, tcp).await?;
                Ok(Self::Tls(Box::new(tls)))
            }
        }
    }
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Build a default rustls client configuration loaded with the system trust anchors.
///
/// Used when the caller of [`crate::Client::connect`] does not supply a custom config.
///
/// The rustls crypto provider is picked by the workspace's `crypto-*`
/// feature (issue #9, ADR-0035) and installed via the explicit
/// [`crate::tls_crypto::active_provider`] shim — no implicit
/// `get_default()` fallback to `ring`.
///
/// # Errors
///
/// Returns [`ClientError::Other`] if the system root certificates cannot be loaded.
pub fn default_tls_config() -> Result<Arc<rustls::ClientConfig>, ClientError> {
    let mut roots = rustls::RootCertStore::empty();
    // rustls-native-certs 0.8 returns a `CertificateResult` that surfaces both the parsed
    // certificates and any per-cert errors. Mirror rustls' own guidance: individual parse
    // failures are non-fatal, but we still bail out if every cert failed to load.
    let native = rustls_native_certs::load_native_certs();
    if native.certs.is_empty() && !native.errors.is_empty() {
        return Err(ClientError::Other(format!(
            "failed to load native root certificates: {:?}",
            native.errors
        )));
    }
    for cert in native.certs {
        let _ = roots.add(cert);
    }
    let config = rustls::ClientConfig::builder_with_provider(crate::tls_crypto::active_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            ClientError::Other(format!(
                "rustls rejected the workspace's default protocol versions: {e}"
            ))
        })?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;
    use crate::dns::{DnsResolveFuture, DnsResolver};
    use crate::url_parse::ParsedUrl;

    /// Resolver that always returns the same caller-supplied address. Used to confirm that
    /// `connect_with_resolver` routes the dial through the resolver's address instead of
    /// the URL's host/port pair.
    #[derive(Debug)]
    struct StaticIpResolver(SocketAddr);

    impl DnsResolver for StaticIpResolver {
        fn resolve<'a>(&'a self, _host: &'a str, _port: u16) -> DnsResolveFuture<'a> {
            let addr = self.0;
            Box::pin(async move { Ok(vec![addr]) })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn connect_with_resolver_uses_resolver_address() {
        // Bind on 127.0.0.1:0 to grab a free port for the URL, then point the resolver
        // at the unreachable port 65535 on the loopback. The resolver wins: we should see
        // a connection refused (I/O) error against 127.0.0.1:65535, NOT against the URL's
        // (effectively meaningless) host.
        let unreachable = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 65535);
        let resolver = StaticIpResolver(unreachable);
        // URL host is intentionally garbage — if the resolver is not honoured we would get
        // a DNS lookup error pointing at "should-be-ignored.invalid", not a connection
        // refused on 127.0.0.1.
        let url = ParsedUrl::parse("pulsar://should-be-ignored.invalid:6650").expect("parse");

        let err = Transport::connect_with_resolver(&url, None, Some(&resolver))
            .await
            .expect_err("connection to 127.0.0.1:65535 must fail");

        // Must surface as an I/O error (connection refused / reset), not the DNS-failure
        // `ClientError::Other` path. That confirms the resolver address actually reached
        // `TcpStream::connect`.
        assert!(
            matches!(err, ClientError::Io(_)),
            "expected ClientError::Io, got {err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn connect_with_resolver_none_falls_back_to_url() {
        // No resolver — the URL host must be used. Pick an obviously-unreachable port on
        // loopback so we still get a fast I/O error rather than waiting on real DNS.
        let url = ParsedUrl::parse("pulsar://127.0.0.1:65535").expect("parse");
        let err = Transport::connect_with_resolver(&url, None, None)
            .await
            .expect_err("connection to 127.0.0.1:65535 must fail");
        assert!(
            matches!(err, ClientError::Io(_)),
            "expected ClientError::Io, got {err:?}"
        );
    }
}
