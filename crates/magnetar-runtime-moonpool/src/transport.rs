// SPDX-License-Identifier: Apache-2.0

//! Transport layer for the moonpool engine.
//!
//! Wraps either a plaintext
//! [`moonpool_core::NetworkProvider::TcpStream`] or that same stream paired
//! with a [`crate::tls::RustlsByteAdapter`] (rustls-over-bytepipe) and exposes
//! the read/write surface the driver loop needs.
//!
//! The underlying stream is already `AsyncRead + AsyncWrite + Unpin`, so the
//! plaintext path is little more than a typed alias — the value is in keeping
//! the engine generic over `P: Providers` without leaking
//! `tokio::net::TcpStream` everywhere. The TLS path drives
//! [`rustls::ClientConnection`] in sans-io fashion: every wire-side read
//! pushes encrypted bytes into the adapter and surfaces decrypted plaintext;
//! every plaintext write queues bytes into the adapter, asks rustls to
//! encrypt, and ships the ciphertext on the wire. This keeps the TLS
//! handshake deterministic under `moonpool-sim` chaos testing — option (d)
//! from `docs/decisions-log.md`, atomised as
//! [ADR-0006](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0006-moonpool-tls-byte-pipe.md).

use std::io;
use std::sync::Arc;

use bytes::BytesMut;
use moonpool_core::{NetworkProvider, Providers};
use rustls::ClientConnection;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::EngineError;
use crate::dns::DnsResolver;
use crate::tls::RustlsByteAdapter;

/// Size of the per-read buffer used by the TLS variant when pulling bytes
/// off the wire before handing them to [`RustlsByteAdapter`]. Sized to fit
/// a single TLS record without spilling, but the buffer grows on demand if
/// rustls needs more.
const TLS_WIRE_BUFFER: usize = 16 * 1024;

/// A connection to a Pulsar broker produced by the configured
/// [`moonpool_core::Providers`]. Owned by the driver task — one transport
/// per connection, never shared.
///
/// Either a plaintext stream or a TLS session running over the same stream
/// type. The enum lets `driver_loop_inner` stay generic over `P` without
/// caring about whether TLS is wrapped on top.
pub(crate) enum Transport<P: Providers> {
    /// Plaintext `pulsar://` connection — `read_buf` / `write_all` pass
    /// through directly to the [`moonpool_core::NetworkProvider::TcpStream`].
    Plain {
        /// The underlying byte pipe.
        stream: <P::Network as NetworkProvider>::TcpStream,
    },
    /// TLS `pulsar+ssl://` connection — same byte pipe wrapped in a
    /// [`RustlsByteAdapter`]. The plaintext driver loop sees only decrypted
    /// bytes; ciphertext travels over `stream` as a side-effect of the
    /// adapter's `step()`.
    Tls {
        /// The underlying byte pipe carrying TLS records.
        stream: <P::Network as NetworkProvider>::TcpStream,
        /// rustls-over-bytepipe adapter — boxed so the enum size stays
        /// reasonable (the adapter carries four BytesMut buffers).
        adapter: Box<RustlsByteAdapter>,
        /// Scratch buffer for `read_buf` to land plaintext into when the
        /// caller's buffer fills up — we may decrypt more bytes than the
        /// caller asked for in a single `read_buf` call.
        plaintext_overflow: BytesMut,
    },
}

impl<P: Providers> Transport<P> {
    /// Establish a plaintext connection to `addr` (a moonpool-format
    /// `host:port` string, NOT a `pulsar://` URL).
    ///
    /// # Errors
    /// Surfaces the underlying [`NetworkProvider::connect`] failure as
    /// [`EngineError::Io`].
    pub(crate) async fn connect(network: &P::Network, addr: &str) -> Result<Self, EngineError> {
        let stream = network.connect(addr).await.map_err(EngineError::Io)?;
        Ok(Self::Plain { stream })
    }

    /// Establish a plaintext connection, routing host resolution through
    /// `resolver` when `Some`. Mirrors the tokio engine's
    /// `Transport::connect_with_resolver` — the resolver returns one or
    /// more candidate [`std::net::SocketAddr`]s and we dial each in order,
    /// returning the first that connects. If every candidate fails, the
    /// last [`std::io::Error`] is surfaced.
    ///
    /// `addr` must parse as `host:port`. When `resolver` is `None`, falls
    /// back to [`Self::connect`] (which routes through the moonpool
    /// [`NetworkProvider`] directly).
    ///
    /// # Errors
    /// - [`EngineError::Config`] when `addr` does not parse as `host:port`.
    /// - [`EngineError::Io`] when every resolved candidate fails to connect.
    pub(crate) async fn connect_with_resolver(
        network: &P::Network,
        addr: &str,
        resolver: Option<&dyn DnsResolver>,
    ) -> Result<Self, EngineError> {
        let Some(resolver) = resolver else {
            return Self::connect(network, addr).await;
        };
        let (host, port) = split_host_port(addr)?;
        let addrs = resolver.resolve(host, port).await?;
        if addrs.is_empty() {
            return Err(EngineError::Config(format!(
                "dns resolver returned no addresses for {host}:{port}"
            )));
        }
        let mut last_err: Option<io::Error> = None;
        for sa in addrs {
            let formatted = sa.to_string();
            match network.connect(&formatted).await {
                Ok(stream) => return Ok(Self::Plain { stream }),
                Err(e) => last_err = Some(e),
            }
        }
        Err(EngineError::Io(
            last_err.expect("at least one connect attempt was made"),
        ))
    }

    /// Establish a TLS connection — dial `addr` via the
    /// [`moonpool_core::NetworkProvider`] (optionally routed through
    /// `resolver`), then drive the rustls handshake over the resulting byte
    /// pipe via [`RustlsByteAdapter`]. The handshake completes inline before
    /// the function returns — callers see an already-handshaken TLS session.
    ///
    /// `host` is the SNI / hostname-verification name (NOT the resolved
    /// IP). `tls_config` is the workspace-wide
    /// [`rustls::ClientConfig`] — there is no `native-tls` or `openssl`
    /// shim ([ADR-0005](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0005-rustls-only-tls.md)).
    ///
    /// # Errors
    /// - [`EngineError::Config`] when `host` is not a valid `ServerName`.
    /// - [`EngineError::Tls`] for any rustls handshake failure (bad cert, version mismatch, …).
    /// - [`EngineError::Io`] for socket failures during the handshake.
    /// - [`EngineError::PeerClosed`] if the peer closes the byte pipe mid-handshake.
    pub(crate) async fn connect_tls(
        network: &P::Network,
        addr: &str,
        host: &str,
        tls_config: Arc<rustls::ClientConfig>,
        resolver: Option<&dyn DnsResolver>,
    ) -> Result<Self, EngineError> {
        let plain = Self::connect_with_resolver(network, addr, resolver).await?;
        let stream = match plain {
            Self::Plain { stream } => stream,
            Self::Tls { .. } => unreachable!("connect_with_resolver only yields Plain"),
        };
        let server_name = ServerName::try_from(host.to_owned()).map_err(|err| {
            EngineError::Config(format!("invalid TLS server name {host:?}: {err}"))
        })?;
        let session = ClientConnection::new(tls_config, server_name).map_err(EngineError::Tls)?;
        let mut transport = Self::Tls {
            stream,
            adapter: Box::new(RustlsByteAdapter::new(session)),
            plaintext_overflow: BytesMut::with_capacity(TLS_WIRE_BUFFER),
        };
        // Drive the handshake to completion. The adapter is stateful: pump
        // outbound ciphertext, pull inbound, repeat until rustls reports
        // `!is_handshaking()`.
        transport.tls_handshake().await?;
        Ok(transport)
    }

    /// Run the rustls handshake to completion. Pumps ciphertext between the
    /// underlying byte pipe and the [`RustlsByteAdapter`] until the adapter
    /// reports `!is_handshaking()`. The plaintext channel is empty when this
    /// returns — the caller's first `write_all` is the first application
    /// payload to traverse the encrypted channel.
    async fn tls_handshake(&mut self) -> Result<(), EngineError> {
        let Self::Tls {
            stream, adapter, ..
        } = self
        else {
            return Ok(());
        };
        // Kick the adapter once to queue the ClientHello.
        adapter.step().map_err(EngineError::Tls)?;
        let mut wire_buf = BytesMut::with_capacity(TLS_WIRE_BUFFER);
        while adapter.is_handshaking() {
            // Push any ciphertext rustls has buffered for the wire.
            let out = adapter.take_encrypted_outbound();
            if !out.is_empty() {
                stream.write_all(&out).await.map_err(EngineError::Io)?;
                stream.flush().await.map_err(EngineError::Io)?;
            }
            if !adapter.is_handshaking() {
                break;
            }
            // Pull more ciphertext off the wire.
            wire_buf.clear();
            let n = stream
                .read_buf(&mut wire_buf)
                .await
                .map_err(EngineError::Io)?;
            if n == 0 {
                return Err(EngineError::PeerClosed);
            }
            adapter.push_encrypted(&wire_buf);
            adapter.step().map_err(EngineError::Tls)?;
        }
        // One final pump to drain any post-handshake bytes (e.g. NewSessionTicket).
        let trailing = adapter.take_encrypted_outbound();
        if !trailing.is_empty() {
            stream.write_all(&trailing).await.map_err(EngineError::Io)?;
            stream.flush().await.map_err(EngineError::Io)?;
        }
        Ok(())
    }

    /// Read up to `buf.len()` bytes from the wire. Returns `0` on a clean EOF.
    ///
    /// Provided for symmetry with the documented transport surface and for
    /// the TLS adapter path, which reads into a fixed-size buffer before
    /// handing bytes to [`RustlsByteAdapter`]. The plain driver uses
    /// [`Self::read_buf`] instead.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncRead::poll_read` error.
    #[allow(dead_code)]
    pub(crate) async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain { stream } => stream.read(buf).await,
            Self::Tls { .. } => {
                // The TLS path goes through `read_buf` — wrap the slice in a
                // BytesMut for symmetry.
                let mut tmp = BytesMut::with_capacity(buf.len());
                let n = self.read_buf(&mut tmp).await?;
                let to_copy = n.min(buf.len());
                buf[..to_copy].copy_from_slice(&tmp[..to_copy]);
                Ok(to_copy)
            }
        }
    }

    /// Read into a [`bytes::BytesMut`]. For plaintext transports this is a
    /// direct passthrough; for TLS transports it pulls ciphertext from the
    /// wire, decrypts via [`RustlsByteAdapter::step`], and lands the
    /// plaintext into `buf`. Returns `0` on a clean EOF.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncRead::poll_read` error and rustls
    /// decrypt failures (translated to [`io::ErrorKind::InvalidData`]).
    pub(crate) async fn read_buf(&mut self, buf: &mut bytes::BytesMut) -> io::Result<usize> {
        match self {
            Self::Plain { stream } => stream.read_buf(buf).await,
            Self::Tls {
                stream,
                adapter,
                plaintext_overflow,
            } => {
                // 1. Drain any plaintext we previously decoded but couldn't fit.
                if !plaintext_overflow.is_empty() {
                    let n = plaintext_overflow.len();
                    buf.extend_from_slice(plaintext_overflow);
                    plaintext_overflow.clear();
                    return Ok(n);
                }
                // 2. Pull ciphertext off the wire, then keep stepping until rustls yields plaintext
                //    (or the peer closes).
                let mut wire = BytesMut::with_capacity(TLS_WIRE_BUFFER);
                let read_n = stream.read_buf(&mut wire).await?;
                if read_n == 0 {
                    return Ok(0);
                }
                adapter.push_encrypted(&wire);
                adapter
                    .step()
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                let plaintext = adapter.take_plaintext();
                if plaintext.is_empty() {
                    // Mid-handshake / rekey traffic — nothing to surface yet.
                    // The driver will park on the read again.
                    return Ok(0);
                }
                buf.extend_from_slice(&plaintext);
                Ok(plaintext.len())
            }
        }
    }

    /// Write the entire `buf` to the wire, looping over short writes.
    /// For TLS transports, queues `buf` through the
    /// [`RustlsByteAdapter`] for encryption and ships the resulting
    /// ciphertext.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_write` error and rustls
    /// encryption failures (translated to [`io::ErrorKind::InvalidData`]).
    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            Self::Plain { stream } => stream.write_all(buf).await,
            Self::Tls {
                stream, adapter, ..
            } => {
                adapter.push_plaintext(buf);
                adapter
                    .step()
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                let ciphertext = adapter.take_encrypted_outbound();
                if !ciphertext.is_empty() {
                    stream.write_all(&ciphertext).await?;
                }
                Ok(())
            }
        }
    }

    /// Flush any buffered bytes. For TLS transports, also pumps any pending
    /// outbound ciphertext.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_flush` error.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain { stream } => stream.flush().await,
            Self::Tls {
                stream, adapter, ..
            } => {
                adapter
                    .step()
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                let pending = adapter.take_encrypted_outbound();
                if !pending.is_empty() {
                    stream.write_all(&pending).await?;
                }
                stream.flush().await
            }
        }
    }

    /// Shut the stream down cleanly. Errors here are non-fatal (the driver
    /// only attempts a shutdown on the happy path), so callers typically
    /// `let _ = transport.shutdown().await;`.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_shutdown` error.
    pub(crate) async fn shutdown(&mut self) -> io::Result<()> {
        // The two arms look identical but resolve `shutdown` against different
        // concrete types (`tokio::io::AsyncWriteExt::shutdown` on the moonpool
        // `TcpStream` vs the `rustls`-wrapped stream) — clippy can't see that.
        #[allow(clippy::match_same_arms)]
        match self {
            Self::Plain { stream } => stream.shutdown().await,
            Self::Tls { stream, .. } => stream.shutdown().await,
        }
    }
}

/// Split a `host:port` literal into its components. Mirrors the trivial
/// parsing that moonpool's [`NetworkProvider::connect`] does internally but
/// surfaces a typed error so the resolver path can report a friendlier
/// configuration mistake. Brackets around IPv6 hosts are stripped.
fn split_host_port(addr: &str) -> Result<(&str, u16), EngineError> {
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| EngineError::Config(format!("invalid host:port literal {addr:?}")))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let port: u16 = port
        .parse()
        .map_err(|e| EngineError::Config(format!("invalid port in {addr:?}: {e}")))?;
    Ok((host, port))
}

impl<P: Providers> std::fmt::Debug for Transport<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain { .. } => f.debug_struct("Transport::Plain").finish_non_exhaustive(),
            Self::Tls { adapter, .. } => f
                .debug_struct("Transport::Tls")
                .field("is_handshaking", &adapter.is_handshaking())
                .finish_non_exhaustive(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::split_host_port;

    #[test]
    fn split_host_port_parses_plain() {
        let (host, port) = split_host_port("broker:6650").expect("parse");
        assert_eq!(host, "broker");
        assert_eq!(port, 6650);
    }

    #[test]
    fn split_host_port_strips_ipv6_brackets() {
        let (host, port) = split_host_port("[::1]:6650").expect("parse");
        assert_eq!(host, "::1");
        assert_eq!(port, 6650);
    }

    #[test]
    fn split_host_port_rejects_missing_port() {
        assert!(split_host_port("broker").is_err());
    }

    // `split_host_port` rejection paths beyond "missing port" are
    // worth pinning too: a non-numeric port-suffix should surface a
    // typed `EngineError::Config` rather than panic / parse silently.
    #[test]
    fn split_host_port_rejects_non_numeric_port() {
        let err = split_host_port("broker:abc")
            .expect_err("non-numeric port must surface as a config error");
        assert!(
            format!("{err:?}").contains("port"),
            "error message should mention port: {err:?}",
        );
    }

    #[test]
    fn split_host_port_handles_high_port() {
        let (host, port) = split_host_port("broker:65535").expect("parse");
        assert_eq!(host, "broker");
        assert_eq!(port, 65535);
    }
}
