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
use std::io::IoSlice;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use moonpool_core::{NetworkProvider, Providers, TimeProvider};
use rustls::ClientConnection;
use rustls::pki_types::ServerName;

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
        /// Reusable heap-backed read scratch — `read_into` lands wire bytes
        /// here once per transport (not once per call) before copying into
        /// the caller's `BytesMut`. Owned on the `Transport` so the returned
        /// read future stays small (a `[u8; TLS_WIRE_BUFFER]` on the stack
        /// trips clippy's `large_futures`); see [`Self::read_into`].
        read_scratch: Box<[u8]>,
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
        /// Reusable heap-backed wire scratch — ciphertext pulled off the
        /// wire lands here before being handed to [`RustlsByteAdapter`],
        /// reused across reads and across the handshake. See the rationale
        /// on [`Self::read_into`] for why it lives here rather than on the
        /// stack.
        read_scratch: Box<[u8]>,
    },
}

impl<P: Providers> Transport<P> {
    /// Perform a single `poll_read` into `buf`, mirroring tokio's
    /// `AsyncReadExt::read_buf` (which `futures::io::AsyncReadExt` does
    /// not provide). One read, `0` == EOF, matching the single-`poll_read`
    /// semantics the old `stream.read_buf(&mut buf)` calls relied on.
    ///
    /// The scratch is owned by the caller (a reusable `Box<[u8]>` field on
    /// the `Transport`) rather than allocated per call: the old in-place
    /// `read_buf` read into the buffer's spare capacity with no extra alloc,
    /// and this restores that. The scratch is *not* a `[u8; TLS_WIRE_BUFFER]`
    /// on the stack — that would carry a 16 KiB frame across the `.await` and
    /// trip clippy's `large_futures` once this helper got inlined into the
    /// handshake / read futures. Passing a `&mut [u8]` keeps the returned
    /// future pointer-sized.
    async fn read_into<S: futures::io::AsyncRead + Unpin>(
        stream: &mut S,
        scratch: &mut [u8],
        buf: &mut BytesMut,
    ) -> io::Result<usize> {
        let n = stream.read(scratch).await?;
        buf.extend_from_slice(&scratch[..n]);
        Ok(n)
    }

    /// Establish a plaintext connection to `addr` (a moonpool-format
    /// `host:port` string, NOT a `pulsar://` URL).
    ///
    /// # Errors
    /// Surfaces the underlying [`NetworkProvider::connect`] failure as
    /// [`EngineError::Io`].
    pub(crate) async fn connect(
        network: &P::Network,
        addr: &str,
        time: &P::Time,
        connect_timeout: Duration,
    ) -> Result<Self, EngineError> {
        // Per-operation dial record — `debug!` per ADR-0054 §2.1; failures
        // are logged by the callers (supervisor / connect retry). Moonpool
        // twin of the tokio `Transport::connect_with_resolver` record; the
        // TLS upgrade (when any) is logged by `connect_tls` below.
        tracing::debug!(
            addr = %addr,
            tls = false,
            connect_timeout_ms = u64::try_from(connect_timeout.as_millis()).unwrap_or(u64::MAX),
            "dialling broker"
        );
        // Single chokepoint for every dial site (initial connect, the proxy /
        // multi-broker pool, and the supervisor reconnect): bound
        // `NetworkProvider::connect` with the engine `TimeProvider` so a hung
        // dial — moonpool-sim's `ConnectFailureMode` connect-hang, or a real
        // broker that stalls mid-establish — is abandoned under virtual time
        // instead of parking forever, surfacing as `Io(TimedOut)` for the
        // caller's retry/backoff to act on. (ADR-0052)
        let connect_fut = network.connect(addr);
        tokio::pin!(connect_fut);
        let stream = tokio::select! {
            biased;
            res = &mut connect_fut => res,
            _ = time.sleep(connect_timeout) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("connect dial to {addr} exceeded connect_timeout ({connect_timeout:?})"),
            )),
        }
        .map_err(EngineError::Io)?;
        Ok(Self::Plain {
            stream,
            read_scratch: new_read_scratch(),
        })
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
        time: &P::Time,
        connect_timeout: Duration,
    ) -> Result<Self, EngineError> {
        let Some(resolver) = resolver else {
            return Self::connect(network, addr, time, connect_timeout).await;
        };
        let (host, port) = split_host_port(addr)?;
        let addrs = resolver.resolve(host, port).await?;
        if addrs.is_empty() {
            return Err(EngineError::Config(format!(
                "dns resolver returned no addresses for {host}:{port}"
            )));
        }
        let mut last_err: Option<EngineError> = None;
        for sa in addrs {
            let formatted = sa.to_string();
            // Each candidate dial inherits the chokepoint timeout via `connect`.
            match Self::connect(network, &formatted, time, connect_timeout).await {
                Ok(transport) => return Ok(transport),
                Err(e) => last_err = Some(e),
            }
        }
        // State-consistency postcondition (mirrors the tokio engine's
        // `connect_with_resolver_inner`): `addrs` was checked non-empty above, so the dial
        // loop ran at least once; falling out of it without an early `Ok` return means every
        // candidate failed and therefore recorded a `last_err`. Cannot fire on legitimate
        // broker/DNS input — only a refactor that drops the non-empty guard. The
        // `unwrap_or_else` fallback below stays as the release-mode safety net.
        debug_assert!(
            last_err.is_some(),
            "all-candidates-failed arm reached without recording any connect error",
        );
        Err(last_err.unwrap_or_else(|| {
            EngineError::Io(io::Error::new(
                io::ErrorKind::NotConnected,
                "no resolved candidate could be dialled",
            ))
        }))
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
        time: &P::Time,
        connect_timeout: Duration,
    ) -> Result<Self, EngineError> {
        // TLS-upgrade record (ADR-0054) — pairs with the plain dial record
        // emitted inside `connect` / `connect_with_resolver`.
        tracing::debug!(
            addr = %addr,
            host = %host,
            tls = true,
            connect_timeout_ms = u64::try_from(connect_timeout.as_millis()).unwrap_or(u64::MAX),
            "dialling broker"
        );
        let plain =
            Self::connect_with_resolver(network, addr, resolver, time, connect_timeout).await?;
        let stream = match plain {
            Self::Plain { stream, .. } => stream,
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
            read_scratch: new_read_scratch(),
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
            stream,
            adapter,
            read_scratch,
            ..
        } = self
        else {
            return Ok(());
        };
        // Kick the adapter once to queue the ClientHello.
        adapter.step().map_err(EngineError::Tls)?;
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
            // Pull more ciphertext off the wire directly into the reusable
            // scratch — no intermediate `BytesMut` copy. Mirrors the TLS
            // arm in `read_buf`.
            let n = stream.read(read_scratch).await.map_err(EngineError::Io)?;
            if n == 0 {
                return Err(EngineError::PeerClosed);
            }
            adapter.push_encrypted(&read_scratch[..n]);
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
            Self::Plain {
                stream,
                read_scratch,
            } => Self::read_into(stream, read_scratch, buf).await,
            Self::Tls {
                stream,
                adapter,
                plaintext_overflow,
                read_scratch,
            } => {
                // 1. Drain any plaintext we previously decoded but couldn't fit.
                if !plaintext_overflow.is_empty() {
                    let n = plaintext_overflow.len();
                    buf.extend_from_slice(plaintext_overflow);
                    plaintext_overflow.clear();
                    return Ok(n);
                }
                // 2. Pull ciphertext off the wire and keep looping until rustls surfaces
                //    application plaintext (or the peer closes). Post-handshake messages such as
                //    `NewSessionTicket` (TLS 1.3) decrypt to nothing user-visible — they bump
                //    `take_plaintext` to empty but `read_n` to non-zero. Returning `Ok(0)` here
                //    would mis-signal EOF to the caller (the driver treats `0` as `PeerClosed`), so
                //    we re-issue the wire read until we either have plaintext or the peer actually
                //    drops.
                loop {
                    // Land ciphertext directly into the reusable scratch and
                    // hand the filled prefix to the adapter — no per-iteration
                    // heap allocation.
                    let read_n = stream.read(read_scratch).await?;
                    if read_n == 0 {
                        return Ok(0);
                    }
                    adapter.push_encrypted(&read_scratch[..read_n]);
                    adapter
                        .step()
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                    let plaintext = adapter.take_plaintext();
                    if !plaintext.is_empty() {
                        buf.extend_from_slice(&plaintext);
                        return Ok(plaintext.len());
                    }
                    // Plaintext empty but wire produced bytes — keep
                    // looping. Common cause: TLS 1.3 NewSessionTicket
                    // arrives post-handshake and is consumed silently.
                    // Looping rather than returning `Ok(0)` matches the
                    // tokio engine's `tokio_rustls::TlsStream` semantics
                    // (which transparently retries on internal records).
                }
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
            Self::Plain { stream, .. } => stream.write_all(buf).await,
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

    /// Write every segment in `segs` to the wire, preserving segment
    /// boundaries on the Plain arm via real `write_vectored`. The bytes on
    /// the wire are byte-identical to coalescing into one buffer — vectored
    /// only skips the user-space coalesce memcpy. Mirrors the tokio engine's
    /// `write_all_vectored` (ADR-0040 wave 2).
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_write_vectored` error and
    /// rustls encryption failures (translated to [`io::ErrorKind::InvalidData`]).
    /// A `write_vectored` returning `0` with a non-empty slice list surfaces
    /// as [`io::ErrorKind::WriteZero`] so the driver doesn't spin.
    pub(crate) async fn write_all_vectored(&mut self, segs: &[bytes::Bytes]) -> io::Result<()> {
        match self {
            Self::Plain { stream, .. } => {
                // Real segment-granular writev: moonpool's `SimTcpStream`
                // records each `IoSlice` as its own ordered delivery event,
                // so the chaos pack can drop / reorder at segment boundaries.
                // `TokioProviders`' `Compat` stream lacks vectored
                // forwarding and falls back to a single-buffer `poll_write`
                // (still correct, just no syscall reduction).
                let mut offsets: Vec<usize> = vec![0; segs.len()];
                loop {
                    let slices: Vec<IoSlice<'_>> = segs
                        .iter()
                        .zip(offsets.iter())
                        .filter_map(|(seg, &off)| {
                            let rest = &seg[off..];
                            if rest.is_empty() {
                                None
                            } else {
                                Some(IoSlice::new(rest))
                            }
                        })
                        .collect();
                    if slices.is_empty() {
                        return Ok(());
                    }
                    let n = stream.write_vectored(&slices).await?;
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "write_vectored returned 0 with non-empty IoSlice array",
                        ));
                    }
                    let mut remaining = n;
                    for (seg, off) in segs.iter().zip(offsets.iter_mut()) {
                        let avail = seg.len().saturating_sub(*off);
                        if avail == 0 {
                            continue;
                        }
                        if remaining >= avail {
                            *off = seg.len();
                            remaining -= avail;
                        } else {
                            *off += remaining;
                            remaining = 0;
                            break;
                        }
                    }
                    debug_assert_eq!(remaining, 0, "kernel reported more bytes than queued");
                }
            }
            Self::Tls {
                stream, adapter, ..
            } => {
                // TLS stays semantically contiguous: rustls owns its own
                // record buffering, so segment boundaries cannot survive
                // encryption. Push each segment's plaintext through the
                // adapter, then ship the resulting ciphertext.
                for seg in segs {
                    adapter.push_plaintext(seg);
                }
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
            Self::Plain { stream, .. } => stream.flush().await,
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
        // The two arms look identical but resolve `close` against different
        // concrete types (`futures::io::AsyncWriteExt::close` on the moonpool
        // `TcpStream` vs the `rustls`-wrapped stream) — clippy can't see that.
        #[allow(clippy::match_same_arms)]
        match self {
            Self::Plain { stream, .. } => stream.close().await,
            Self::Tls { stream, .. } => stream.close().await,
        }
    }
}

/// Allocate the reusable per-transport read scratch. A heap-backed
/// `Box<[u8]>` of [`TLS_WIRE_BUFFER`] bytes, reused across every wire read
/// for the life of the transport so `read_into` no longer allocates per
/// call. Lives on the heap (not the stack) so the returned read future
/// stays small — see [`Transport::read_into`].
fn new_read_scratch() -> Box<[u8]> {
    vec![0u8; TLS_WIRE_BUFFER].into_boxed_slice()
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

    // =====================================================================
    // ADR-0040 wave 2 — `Transport::write_all_vectored` Plain arm over a
    // real `moonpool-sim` `SimTcpStream`. `Transport` is `pub(crate)`, so
    // these live in-crate rather than under `tests/`. They drive the same
    // `write_vectored` path the moonpool driver dispatches `TransmitOwned
    // ::Vectored` through (ADR-0024 layer (c) for the moonpool engine), and
    // exercise the offset-tracking short-count loop that the byte-identical
    // e2e produce path can't deterministically hit.
    // =====================================================================
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use futures::io::{AsyncRead, AsyncWriteExt};
    use moonpool_core::{NetworkProvider, TcpListenerTrait};
    use moonpool_sim::providers::SimProviders;
    use moonpool_sim::{NetworkConfiguration, SimWorld};

    use super::Transport;

    /// One non-blocking `poll_read` into `buf`, returning the byte count on
    /// a `Ready(Ok(n>0))` and `None` otherwise. Mirrors the helper in
    /// moonpool-sim's own `network/vectored.rs`.
    fn try_read(server: &mut (impl AsyncRead + Unpin), buf: &mut [u8]) -> Option<usize> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(server).poll_read(&mut cx, buf) {
            Poll::Ready(Ok(n)) if n > 0 => Some(n),
            _ => None,
        }
    }

    /// Small multi-segment vectored write completes in a single
    /// `poll_write_vectored` (the 64 KiB send buffer has room), and the sim
    /// records each `IoSlice` as its own ordered delivery event — so the
    /// server reads the segments back as distinct chunks in order. Proves
    /// the Plain arm performs a *real* segment-granular writev, not a
    /// coalescing fallback.
    #[test]
    fn write_all_vectored_plain_delivers_segments_in_order() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("build current-thread runtime");

        rt.block_on(async move {
            let mut sim = SimWorld::new_with_network_config(NetworkConfiguration::fast_local());
            let provider = sim.network_provider();
            let addr = "vectored-segments";

            let listener = provider.bind(addr).await.expect("bind");
            let client_stream = provider.connect(addr).await.expect("connect");
            let (mut server, _peer) = listener.accept().await.expect("accept");

            let mut transport: Transport<SimProviders> = Transport::Plain {
                stream: client_stream,
                read_scratch: super::new_read_scratch(),
            };

            let segs = vec![
                Bytes::from_static(b"AAAA"),
                Bytes::from_static(b"BBBBBB"),
                Bytes::from_static(b"CC"),
            ];
            let total: usize = segs.iter().map(Bytes::len).sum();
            transport
                .write_all_vectored(&segs)
                .await
                .expect("vectored write");

            // Drain the sim, collecting each delivery event as a chunk.
            let mut chunks: Vec<Vec<u8>> = Vec::new();
            let mut buf = vec![0u8; 4096];
            while sim.pending_event_count() > 0 {
                sim.step();
                if let Some(n) = try_read(&mut server, &mut buf) {
                    chunks.push(buf[..n].to_vec());
                }
            }

            assert_eq!(
                chunks,
                vec![b"AAAA".to_vec(), b"BBBBBB".to_vec(), b"CC".to_vec()],
                "each IoSlice must surface as its own ordered delivery event",
            );
            let reassembled: Vec<u8> = chunks.concat();
            assert_eq!(reassembled.len(), total);
        });
    }

    /// Segments whose combined length exceeds the sim's 64 KiB send buffer
    /// force a short `write_vectored` (partial accept). The Plain arm's
    /// offset-tracking loop must re-issue the writev for the unflushed tail
    /// until every byte lands — and the reassembled stream on the server
    /// must equal the concatenation of all segments, byte-for-byte.
    #[test]
    fn write_all_vectored_plain_handles_partial_accept() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("build current-thread runtime");

        rt.block_on(async move {
            let mut sim = SimWorld::new_with_network_config(NetworkConfiguration::fast_local());
            let provider = sim.network_provider();
            let addr = "vectored-partial";

            let listener = provider.bind(addr).await.expect("bind");
            let client_stream = provider.connect(addr).await.expect("connect");
            let (mut server, _peer) = listener.accept().await.expect("accept");

            // Three segments totalling 96 KiB > the 64 KiB send buffer, so
            // the first writev cannot accept everything and the loop must
            // advance offsets across re-issues. Distinct fill bytes per
            // segment let us assert the reassembled order.
            let seg_len = 32 * 1024;
            let segs = vec![
                Bytes::from(vec![1u8; seg_len]),
                Bytes::from(vec![2u8; seg_len]),
                Bytes::from(vec![3u8; seg_len]),
            ];
            let mut expected: Vec<u8> = Vec::with_capacity(seg_len * 3);
            for s in &segs {
                expected.extend_from_slice(s);
            }
            let total = expected.len();

            // The writer parks on backpressure once the 64 KiB buffer fills;
            // it only completes as the server drains. Spawn it so the main
            // task can step the sim + read concurrently. `SimTcpStream` is
            // `Send`, so a plain `tokio::spawn` on the current-thread runtime
            // works.
            let done = Arc::new(AtomicBool::new(false));
            let done_writer = done.clone();
            let writer = tokio::spawn(async move {
                transport_write_all_vectored(client_stream, segs).await;
                done_writer.store(true, Ordering::SeqCst);
            });

            let mut received: Vec<u8> = Vec::with_capacity(total);
            let mut buf = vec![0u8; 16 * 1024];
            // Bounded loop: step the sim (which polls the parked writer and
            // delivers buffered bytes), drain the server, repeat until the
            // writer finished and every byte arrived. The cap guards against
            // a regression that fails to make progress.
            for _ in 0..100_000 {
                if done.load(Ordering::SeqCst) && received.len() >= total {
                    break;
                }
                sim.step();
                tokio::task::yield_now().await;
                while let Some(n) = try_read(&mut server, &mut buf) {
                    received.extend_from_slice(&buf[..n]);
                }
            }

            writer.await.expect("writer task joined");
            assert_eq!(
                received.len(),
                total,
                "partial-accept loop must flush every byte",
            );
            assert_eq!(
                received, expected,
                "reassembled stream must equal the segment concatenation",
            );
        });
    }

    /// Helper so the spawned writer owns a concrete `Transport::Plain`
    /// without leaking the generic param into the closure capture.
    async fn transport_write_all_vectored(
        stream: <<SimProviders as moonpool_core::Providers>::Network as NetworkProvider>::TcpStream,
        segs: Vec<Bytes>,
    ) {
        let mut transport: Transport<SimProviders> = Transport::Plain {
            stream,
            read_scratch: super::new_read_scratch(),
        };
        transport
            .write_all_vectored(&segs)
            .await
            .expect("vectored write (partial-accept)");
        // Close so the server sees a clean EOF after the last byte.
        let _ = AsyncWriteExt::close(&mut match transport {
            Transport::Plain { stream, .. } => stream,
            Transport::Tls { .. } => unreachable!("constructed Plain"),
        })
        .await;
    }
}
