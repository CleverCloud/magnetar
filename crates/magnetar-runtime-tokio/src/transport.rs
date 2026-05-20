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
    /// ignored.
    pub(crate) async fn connect(
        url: &ParsedUrl,
        tls_config: Option<Arc<rustls::ClientConfig>>,
    ) -> Result<Self, ClientError> {
        let tcp = TcpStream::connect(url.socket_addr()).await?;
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
/// # Errors
///
/// Returns [`ClientError::Other`] if the system root certificates cannot be loaded.
pub(crate) fn default_tls_config() -> Result<Arc<rustls::ClientConfig>, ClientError> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs()
        .map_err(|e| ClientError::Other(format!("failed to load native root certificates: {e}")))?;
    for cert in native {
        // Ignore individual cert parse errors — match rustls own recommendation for trust stores.
        let _ = roots.add(cert);
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}
