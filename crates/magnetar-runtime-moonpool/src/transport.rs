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
//!
//! # Read/write split (ADR-0083)
//!
//! [`Transport::into_split`] consumes the connected transport and returns
//! independently-ownable [`TransportReadHalf`] / [`TransportWriteHalf`]
//! values, called once right after the connection is established (handshake
//! included), so `driver_loop_inner` can hold both halves as separate local
//! bindings and give the write its own `select!` arm without a second
//! `&mut Transport` borrow. The split itself is via
//! [`futures::io::AsyncReadExt::split`] (a `BiLock`-backed, by-value split —
//! NOT `tokio::io::split`, which targets tokio's own `AsyncRead`/`AsyncWrite`
//! traits that this crate's provider streams do not implement). The `Plain`
//! arm needs nothing more: `stream` is the only field and it is
//! direction-exclusive once split. The `Tls` arm is different — encryption
//! state is inherently bidirectional (`RustlsByteAdapter::step()` both
//! drains inbound ciphertext into plaintext AND drains queued outbound
//! plaintext into ciphertext in the same call), so the two TLS halves share
//! one `Arc<parking_lot::Mutex<TlsShared>>`. The mutex is never held across
//! an `.await` — `RustlsByteAdapter::step()` is fully synchronous — so this
//! is the same "never park while holding a `parking_lot` guard" discipline
//! CLAUDE.md invariant #1 already requires elsewhere in this codebase.
//! [`TlsShared::pending_ciphertext`] is the resumable, cancel-safe queue
//! `write_some` and `read_buf` both feed: the write half's own application
//! writes AND any protocol-mandated response the read half's decrypt step
//! produces (a TLS 1.3 `KeyUpdate` acknowledgement, a `close_notify` echo)
//! land in the SAME queue. The read half only ever appends to it — it never
//! touches the socket — so a read-triggered response is not stranded on an
//! otherwise write-idle connection: the driver loop re-evaluates
//! `TransportWriteHalf::has_pending_ciphertext()` at the top of every
//! iteration (the same gate that decides whether the write `select!` arm is
//! even polled), and the just-appended bytes make that gate fire on the very
//! next iteration with no extra wakeup plumbing needed.

use std::io;
#[cfg(test)]
use std::io::IoSlice;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use moonpool_core::{NetworkProvider, Providers, TimeProvider};
use parking_lot::Mutex;
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

/// Perform a single `poll_read` into `buf`, mirroring tokio's
/// `AsyncReadExt::read_buf` (which `futures::io::AsyncReadExt` does not
/// provide). One read, `0` == EOF, matching the single-`poll_read` semantics
/// the old in-place `stream.read_buf(&mut buf)` calls relied on.
///
/// The scratch is owned by the caller (a reusable `Box<[u8]>` field on
/// [`Transport`] / [`TransportReadHalf`]) rather than allocated per call: the
/// old in-place `read_buf` read into the buffer's spare capacity with no
/// extra alloc, and this restores that. The scratch is *not* a
/// `[u8; TLS_WIRE_BUFFER]` on the stack — that would carry a 16 KiB frame
/// across the `.await` and trip clippy's `large_futures` once this helper
/// got inlined into the handshake / read futures. Passing a `&mut [u8]`
/// keeps the returned future pointer-sized. Free function (not a `Transport`
/// method) so both the pre-split `Transport::read_buf` and the post-split
/// `TransportReadHalf::read_buf` share the same body.
async fn read_into<S: futures::io::AsyncRead + Unpin>(
    stream: &mut S,
    scratch: &mut [u8],
    buf: &mut BytesMut,
) -> io::Result<usize> {
    let n = stream.read(scratch).await?;
    buf.extend_from_slice(&scratch[..n]);
    Ok(n)
}

/// Allocate the reusable per-transport read scratch. A heap-backed
/// `Box<[u8]>` of [`TLS_WIRE_BUFFER`] bytes, reused across every wire read
/// for the life of the transport so [`read_into`] no longer allocates per
/// call. Lives on the heap (not the stack) so the returned read future
/// stays small — see [`read_into`].
fn new_read_scratch() -> Box<[u8]> {
    vec![0u8; TLS_WIRE_BUFFER].into_boxed_slice()
}

/// TLS encryption state shared between [`TransportReadHalf`] and
/// [`TransportWriteHalf`] after [`Transport::into_split`], and used
/// exclusively (no `Arc`, no lock) by the not-yet-split [`Transport::Tls`]
/// variant beforehand. See the module docs' "Read/write split" section for
/// why this must be shared rather than split like the `Plain` arm, and for
/// the cancellation-safety invariant `pending_ciphertext` upholds
/// (ADR-0083).
pub(crate) struct TlsShared {
    adapter: RustlsByteAdapter,
    /// Already-encrypted bytes not yet fully accepted by the socket.
    /// Resumable: `write_some` advances `pending_ciphertext_offset` only
    /// after a low-level write actually lands bytes, and NEVER re-invokes
    /// the adapter on data that may already be represented here — so a
    /// `write_some` future dropped mid-poll (a cancelled `select!` write
    /// arm) neither re-encrypts already-encrypted bytes nor loses bytes the
    /// adapter already produced.
    pending_ciphertext: BytesMut,
    pending_ciphertext_offset: usize,
}

impl TlsShared {
    fn new(adapter: RustlsByteAdapter) -> Self {
        Self {
            adapter,
            pending_ciphertext: BytesMut::new(),
            pending_ciphertext_offset: 0,
        }
    }

    /// `true` while there is already-encrypted, not-yet-fully-written data
    /// queued. Read by the driver loop's write-arm `select!` gate — this is
    /// what lets a read-triggered protocol response (see module docs) make
    /// the write arm fire even when the application has nothing new to
    /// send.
    fn has_pending_ciphertext(&self) -> bool {
        self.pending_ciphertext_offset < self.pending_ciphertext.len()
    }

    /// Move whatever the adapter has queued for the wire into the durable
    /// `pending_ciphertext` queue. Called after every `adapter.step()` by
    /// BOTH halves — the read half calls this to surface protocol-mandated
    /// responses without writing to the socket itself (see module docs);
    /// the write half calls this right after pushing new application
    /// plaintext through the adapter.
    fn absorb_adapter_output(&mut self) {
        let extra = self.adapter.take_encrypted_outbound();
        if extra.is_empty() {
            return;
        }
        if self.pending_ciphertext_offset >= self.pending_ciphertext.len() {
            // Already fully drained — start fresh instead of growing the
            // buffer with a dead prefix.
            self.pending_ciphertext.clear();
            self.pending_ciphertext_offset = 0;
        }
        self.pending_ciphertext.extend_from_slice(&extra);
    }

    /// The not-yet-written suffix of `pending_ciphertext`.
    fn remaining_ciphertext(&self) -> &[u8] {
        &self.pending_ciphertext[self.pending_ciphertext_offset..]
    }

    /// Commit `n` more bytes of `pending_ciphertext` as durably written to
    /// the wire. Called ONLY after a low-level write's `Poll::Ready(Ok(n))`,
    /// synchronously, before any further `.await` point — see
    /// [`Self::pending_ciphertext`]'s doc comment.
    fn advance_ciphertext(&mut self, n: usize) {
        self.pending_ciphertext_offset += n;
        if self.pending_ciphertext_offset >= self.pending_ciphertext.len() {
            self.pending_ciphertext.clear();
            self.pending_ciphertext_offset = 0;
        }
    }
}

/// A connection to a Pulsar broker produced by the configured
/// [`moonpool_core::Providers`]. Owned by the driver task — one transport
/// per connection, never shared, UNTIL [`Self::into_split`] hands ownership
/// of the two independently-pollable halves to the driver loop.
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
        /// Reusable heap-backed read scratch — see [`read_into`].
        read_scratch: Box<[u8]>,
    },
    /// TLS `pulsar+ssl://` connection — same byte pipe wrapped in a
    /// [`RustlsByteAdapter`] behind [`TlsShared`]. The plaintext driver loop
    /// sees only decrypted bytes; ciphertext travels over `stream` as a
    /// side-effect of the adapter's `step()`.
    Tls {
        /// The underlying byte pipe carrying TLS records.
        stream: <P::Network as NetworkProvider>::TcpStream,
        /// Already behind `Arc<Mutex<_>>` even before [`Self::into_split`]
        /// so the pre-split methods below and the post-split halves share
        /// one implementation of the cancellation-safe write path — see the
        /// module docs.
        shared: Arc<Mutex<TlsShared>>,
        /// Scratch buffer for `read_buf` to land plaintext into when the
        /// caller's buffer fills up — we may decrypt more bytes than the
        /// caller asked for in a single `read_buf` call.
        plaintext_overflow: BytesMut,
        /// Reusable heap-backed wire scratch — see [`read_into`].
        read_scratch: Box<[u8]>,
    },
}

impl<P: Providers> Transport<P> {
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
        let mut connect_fut = std::pin::pin!(connect_fut);
        let stream = moonpool_core::select! {
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
            shared: Arc::new(Mutex::new(TlsShared::new(RustlsByteAdapter::new(session)))),
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
    ///
    /// Runs entirely before [`Self::into_split`] and entirely before the
    /// driver loop's write-`select!`-arm exists for this connection, so
    /// unlike [`Self::write_some`] there is no cancellation-safety concern
    /// here — handshake bytes go straight to the wire via `write_all`, not
    /// through `pending_ciphertext`.
    async fn tls_handshake(&mut self) -> Result<(), EngineError> {
        let Self::Tls {
            stream,
            shared,
            read_scratch,
            ..
        } = self
        else {
            return Ok(());
        };
        // Kick the adapter once to queue the ClientHello.
        shared.lock().adapter.step().map_err(EngineError::Tls)?;
        loop {
            let (is_handshaking, out) = {
                let mut g = shared.lock();
                (
                    g.adapter.is_handshaking(),
                    g.adapter.take_encrypted_outbound(),
                )
            };
            if !out.is_empty() {
                stream.write_all(&out).await.map_err(EngineError::Io)?;
                stream.flush().await.map_err(EngineError::Io)?;
            }
            if !is_handshaking {
                break;
            }
            // Pull more ciphertext off the wire directly into the reusable
            // scratch — no intermediate `BytesMut` copy. Mirrors the TLS
            // arm in `read_buf`.
            let n = stream.read(read_scratch).await.map_err(EngineError::Io)?;
            if n == 0 {
                return Err(EngineError::PeerClosed);
            }
            let mut g = shared.lock();
            g.adapter.push_encrypted(&read_scratch[..n]);
            g.adapter.step().map_err(EngineError::Tls)?;
        }
        // One final pump to drain any post-handshake bytes (e.g. NewSessionTicket).
        let trailing = shared.lock().adapter.take_encrypted_outbound();
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
            } => read_into(stream, read_scratch, buf).await,
            Self::Tls {
                stream,
                shared,
                plaintext_overflow,
                read_scratch,
            } => read_tls_buf(stream, shared, plaintext_overflow, read_scratch, buf).await,
        }
    }

    /// Perform ONE single-poll write, single-poll on the underlying wire.
    /// For the `Plain` arm this is a direct `poll_write` passthrough. For
    /// the `Tls` arm this is the cancellation-safety primitive (ADR-0083):
    /// see [`write_some_tls`]'s doc comment.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_write` error and rustls
    /// encryption failures (translated to [`io::ErrorKind::InvalidData`]).
    pub(crate) async fn write_some(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain { stream, .. } => write_some_plain(stream, buf).await,
            Self::Tls { stream, shared, .. } => write_some_tls(stream, shared, buf).await,
        }
    }

    /// Write the entire `buf` to the wire, looping over
    /// [`Self::write_some`] until every byte is durably queued (`Tls`) or
    /// physically written (`Plain`), then draining any TLS residue so the
    /// "fully sent" contract non-driver callers (the connect/handshake
    /// bootstrap loop in `lib.rs`) rely on actually holds.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_write` error and rustls
    /// encryption failures (translated to [`io::ErrorKind::InvalidData`]).
    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let mut offset = 0;
        while offset < buf.len() {
            let n = self.write_some(&buf[offset..]).await?;
            offset += n;
        }
        self.drain_pending_ciphertext().await
    }

    /// Drain any TLS `pending_ciphertext` residue to completion. No-op for
    /// `Plain`. See [`write_some_tls`]'s doc comment for why `write_some`
    /// alone does not guarantee bytes are already on the wire.
    async fn drain_pending_ciphertext(&mut self) -> io::Result<()> {
        while let Self::Tls { shared, .. } = self {
            if !shared.lock().has_pending_ciphertext() {
                break;
            }
            let _ = self.write_some(&[]).await?;
        }
        Ok(())
    }

    /// Write every segment in `segs` to the wire, preserving segment
    /// boundaries on the Plain arm via real `write_vectored`. The bytes on
    /// the wire are byte-identical to coalescing into one buffer — vectored
    /// only skips the user-space coalesce memcpy. Mirrors the tokio engine's
    /// `write_all_vectored` (ADR-0040 wave 2). Test-only: the production
    /// driver dispatches `TransmitOwned::Vectored` through
    /// `PendingDriverWrite::write_budgeted` (which flattens into
    /// `write_some`/`write_all` calls), not through this method.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_write_vectored` error and
    /// rustls encryption failures (translated to [`io::ErrorKind::InvalidData`]).
    /// A `write_vectored` returning `0` with a non-empty slice list surfaces
    /// as [`io::ErrorKind::WriteZero`] so the driver doesn't spin.
    #[cfg(test)]
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
            Self::Tls { .. } => {
                // TLS stays semantically contiguous: rustls owns its own
                // record buffering, so segment boundaries cannot survive
                // encryption. Push each segment's plaintext through
                // `write_all`, which itself goes through the shared
                // cancellation-safe `write_some` path.
                for seg in segs {
                    self.write_all(seg).await?;
                }
                Ok(())
            }
        }
    }

    /// Flush any buffered bytes. For TLS transports, also pumps and drains
    /// any pending outbound ciphertext (including any protocol-mandated
    /// response a prior `read_buf` call queued via `absorb_adapter_output`).
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_flush` error.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain { stream, .. } => stream.flush().await,
            Self::Tls { shared, .. } => {
                shared
                    .lock()
                    .adapter
                    .step()
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                shared.lock().absorb_adapter_output();
                self.drain_pending_ciphertext().await?;
                let Self::Tls { stream, .. } = self else {
                    unreachable!("matched Tls above")
                };
                stream.flush().await
            }
        }
    }

    /// Consume the connected transport and split it into independently
    /// ownable read/write halves — see the module docs' "Read/write split"
    /// section. Called exactly once, right after connect (handshake
    /// included), before the driver loop's `select!` starts giving read and
    /// write their own arms.
    pub(crate) fn into_split(self) -> (TransportReadHalf<P>, TransportWriteHalf<P>) {
        match self {
            Self::Plain {
                stream,
                read_scratch,
            } => {
                let (read_half, write_half) = stream.split();
                (
                    TransportReadHalf::Plain {
                        read_half,
                        read_scratch,
                    },
                    TransportWriteHalf::Plain { write_half },
                )
            }
            Self::Tls {
                stream,
                shared,
                plaintext_overflow,
                read_scratch,
            } => {
                let (read_half, write_half) = stream.split();
                (
                    TransportReadHalf::Tls {
                        read_half,
                        shared: shared.clone(),
                        plaintext_overflow,
                        read_scratch,
                    },
                    TransportWriteHalf::Tls { write_half, shared },
                )
            }
        }
    }
}

/// Shared TLS read logic — see [`Transport::read_buf`] /
/// [`TransportReadHalf::read_buf`], which both call this with their own
/// concrete stream-half type.
async fn read_tls_buf<S: futures::io::AsyncRead + Unpin>(
    stream: &mut S,
    shared: &Arc<Mutex<TlsShared>>,
    plaintext_overflow: &mut BytesMut,
    read_scratch: &mut [u8],
    buf: &mut BytesMut,
) -> io::Result<usize> {
    // 1. Drain any plaintext we previously decoded but couldn't fit.
    if !plaintext_overflow.is_empty() {
        let n = plaintext_overflow.len();
        buf.extend_from_slice(plaintext_overflow);
        plaintext_overflow.clear();
        return Ok(n);
    }
    // 2. Pull ciphertext off the wire and keep looping until rustls surfaces application plaintext
    //    (or the peer closes). Post-handshake messages such as `NewSessionTicket` (TLS 1.3) decrypt
    //    to nothing user-visible — they bump `take_plaintext` to empty but `read_n` to non-zero.
    //    Returning `Ok(0)` here would mis-signal EOF to the caller (the driver treats `0` as
    //    `PeerClosed`), so we re-issue the wire read until we either have plaintext or the peer
    //    actually drops.
    loop {
        // Land ciphertext directly into the reusable scratch and hand the
        // filled prefix to the adapter — no per-iteration heap allocation.
        let read_n = stream.read(read_scratch).await?;
        if read_n == 0 {
            return Ok(0);
        }
        let plaintext = {
            let mut g = shared.lock();
            g.adapter.push_encrypted(&read_scratch[..read_n]);
            g.adapter
                .step()
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            // ADR-0083: surface any protocol-mandated outbound response this
            // decrypt step produced (e.g. a TLS 1.3 KeyUpdate ack) into the
            // shared, resumable ciphertext queue — WITHOUT writing to the
            // socket from the read side. The write half's `select!`-arm gate
            // (`has_pending_ciphertext`) picks this up on the next driver-loop
            // iteration — which is NOT necessarily the next instant: we are
            // inside this function's own retry loop, and `write_has_work` was
            // snapshotted before the driver entered `select!`. If the decrypt
            // yielded no plaintext we loop and `.await` another read here, so
            // the queued ciphertext waits for whichever comes first: more wire
            // bytes, a `driver_waker` pulse, or the timer arm. It is therefore
            // bounded by the driver's timer interval, not by one iteration —
            // acceptable because the frames this queues (KeyUpdate acks) are
            // rare and not latency-critical. See the module docs.
            g.absorb_adapter_output();
            g.adapter.take_plaintext()
        };
        if !plaintext.is_empty() {
            buf.extend_from_slice(&plaintext);
            return Ok(plaintext.len());
        }
        // Plaintext empty but wire produced bytes — keep looping. Common
        // cause: TLS 1.3 NewSessionTicket arrives post-handshake and is
        // consumed silently. Looping rather than returning `Ok(0)` matches
        // the tokio engine's `tokio_rustls::TlsStream` semantics (which
        // transparently retries on internal records).
    }
}

/// Shared Plain-arm write logic — see [`Transport::write_some`] /
/// [`TransportWriteHalf::write_some`]. A single `poll_write`, with a
/// non-empty `buf` that comes back `Ok(0)` promoted to
/// [`io::ErrorKind::WriteZero`] — this is the ONLY meaning `Ok(0)` can carry
/// for a raw socket, so callers (`PendingDriverWrite::write_budgeted`) can
/// treat `Ok(0)` from ANY `write_some` implementation (`Plain` or `Tls`) as
/// "no new progress on `buf` this call, loop again" without needing to know
/// which variant they're driving — the `Tls` arm's legitimate "drained
/// residue, no new plaintext consumed" `Ok(0)` (see [`write_some_tls`]
/// below) and this arm's impossible "wrote nothing at all" case are handled
/// at their respective sources instead of being conflated by the caller.
async fn write_some_plain<S: futures::io::AsyncWrite + Unpin>(
    stream: &mut S,
    buf: &[u8],
) -> io::Result<usize> {
    let n = stream.write(buf).await?;
    if n == 0 && !buf.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "driver write: write() returned 0 with a non-empty buffer",
        ));
    }
    Ok(n)
}

/// Shared TLS write logic — see [`Transport::write_some`] /
/// [`TransportWriteHalf::write_some`].
///
/// Cancellation-safety invariant (ADR-0083): this performs AT MOST one
/// low-level `poll_write`, so it is safe for the caller (`select!`) to drop
/// this future between calls without duplicating or losing bytes.
///
/// - If `pending_ciphertext` already has unsent bytes (from a previous call, OR from a
///   read-triggered protocol response — see [`TlsShared::absorb_adapter_output`]), this call does
///   ONLY a single write of (a suffix of) that queue and commits the offset synchronously right
///   after the write resolves, before returning. It reports `Ok(0)` for `buf` in this branch: none
///   of `buf`'s plaintext has been consumed yet, only wire progress on a PRIOR chunk was made.
/// - Otherwise `buf` is encrypted and captured into `pending_ciphertext` ENTIRELY SYNCHRONOUSLY
///   (`push_plaintext` → `step` → `absorb_adapter_output`, no `.await` in between), so this branch
///   can never be cancelled mid-way: either the whole of `buf` becomes durably represented in
///   `pending_ciphertext` and `Ok(buf.len())` is returned, or this function was never entered at
///   all. The physical wire write of that ciphertext happens on a LATER call (this call's own or a
///   subsequent one), via the first branch — deliberately deferred so this branch has no `.await`
///   to be cancelled at.
async fn write_some_tls<S: futures::io::AsyncWrite + Unpin>(
    stream: &mut S,
    shared: &Arc<Mutex<TlsShared>>,
    buf: &[u8],
) -> io::Result<usize> {
    let pending = {
        let g = shared.lock();
        if g.has_pending_ciphertext() {
            Some(Bytes::copy_from_slice(g.remaining_ciphertext()))
        } else {
            None
        }
    };
    if let Some(pending) = pending {
        let n = stream.write(&pending).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "driver write (tls): write() returned 0 with a non-empty buffer",
            ));
        }
        shared.lock().advance_ciphertext(n);
        return Ok(0);
    }

    let mut g = shared.lock();
    g.adapter.push_plaintext(buf);
    g.adapter
        .step()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    g.absorb_adapter_output();
    Ok(buf.len())
}

/// Read half produced by [`Transport::into_split`]. See the module docs'
/// "Read/write split" section.
pub(crate) enum TransportReadHalf<P: Providers> {
    Plain {
        read_half: futures::io::ReadHalf<<P::Network as NetworkProvider>::TcpStream>,
        read_scratch: Box<[u8]>,
    },
    Tls {
        read_half: futures::io::ReadHalf<<P::Network as NetworkProvider>::TcpStream>,
        shared: Arc<Mutex<TlsShared>>,
        plaintext_overflow: BytesMut,
        read_scratch: Box<[u8]>,
    },
}

impl<P: Providers> TransportReadHalf<P> {
    /// Identical contract to [`Transport::read_buf`].
    ///
    /// # Errors
    /// Propagates the underlying `AsyncRead::poll_read` error and rustls
    /// decrypt failures (translated to [`io::ErrorKind::InvalidData`]).
    pub(crate) async fn read_buf(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        match self {
            Self::Plain {
                read_half,
                read_scratch,
            } => read_into(read_half, read_scratch, buf).await,
            Self::Tls {
                read_half,
                shared,
                plaintext_overflow,
                read_scratch,
            } => read_tls_buf(read_half, shared, plaintext_overflow, read_scratch, buf).await,
        }
    }
}

/// Write half produced by [`Transport::into_split`]. See the module docs'
/// "Read/write split" section.
pub(crate) enum TransportWriteHalf<P: Providers> {
    Plain {
        write_half: futures::io::WriteHalf<<P::Network as NetworkProvider>::TcpStream>,
    },
    Tls {
        write_half: futures::io::WriteHalf<<P::Network as NetworkProvider>::TcpStream>,
        shared: Arc<Mutex<TlsShared>>,
    },
}

impl<P: Providers> TransportWriteHalf<P> {
    /// Identical contract to [`Transport::write_some`].
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_write` error and rustls
    /// encryption failures (translated to [`io::ErrorKind::InvalidData`]).
    pub(crate) async fn write_some(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain { write_half } => write_some_plain(write_half, buf).await,
            Self::Tls { write_half, shared } => write_some_tls(write_half, shared, buf).await,
        }
    }

    /// `true` when there is application-queued OR read-triggered ciphertext
    /// still due to reach the wire. Gates the driver loop's write `select!`
    /// arm — see the module docs.
    pub(crate) fn has_pending_ciphertext(&self) -> bool {
        match self {
            Self::Plain { .. } => false,
            Self::Tls { shared, .. } => shared.lock().has_pending_ciphertext(),
        }
    }

    /// Flush any buffered bytes; for TLS, drains `pending_ciphertext` to
    /// completion first (mirrors [`Transport::flush`]).
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_flush` error.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain { write_half } => write_half.flush().await,
            Self::Tls { write_half, shared } => {
                while shared.lock().has_pending_ciphertext() {
                    let _ = write_some_tls(write_half, shared, &[]).await?;
                }
                write_half.flush().await
            }
        }
    }

    /// Shut the write half down cleanly.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_shutdown` error.
    pub(crate) async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            Self::Plain { write_half } | Self::Tls { write_half, .. } => write_half.close().await,
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
            Self::Tls { shared, .. } => f
                .debug_struct("Transport::Tls")
                .field("is_handshaking", &shared.lock().adapter.is_handshaking())
                .finish_non_exhaustive(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

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
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
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

    // =====================================================================
    // ADR-0083 — TLS `write_some` cancellation safety. A dropped
    // `write_some` future must neither duplicate nor lose bytes: bytes
    // already captured into `pending_ciphertext` (synchronously, no await)
    // survive the drop, and bytes already accepted by the wire are never
    // re-encrypted. Mirrors the tokio engine's
    // `write_budgeted_is_cancel_safe_across_a_dropped_await` at the
    // `write_some` primitive layer (this crate's `Transport::write_some` is
    // the direct analogue of the tokio engine's single-poll `write()` call).
    // =====================================================================
    use rustls::ClientConnection;

    use super::{RustlsByteAdapter, TlsShared};
    use crate::tls_crypto;

    /// A minimal, unhandshaked `rustls::ClientConnection` — sufficient to
    /// exercise `push_plaintext` / `step` / `take_encrypted_outbound`
    /// buffer bookkeeping without a real peer. Mirrors `tls.rs`'s own
    /// `make_session` test helper.
    fn make_session() -> ClientConnection {
        tls_crypto::install_default_provider();
        let root_store = rustls::RootCertStore::empty();
        let config = std::sync::Arc::new(
            rustls::ClientConfig::builder_with_provider(tls_crypto::active_provider())
                .with_safe_default_protocol_versions()
                .expect("rustls default protocol versions are valid")
                .with_root_certificates(root_store)
                .with_no_client_auth(),
        );
        let name = rustls::pki_types::ServerName::try_from("example.com").unwrap();
        ClientConnection::new(config, name).expect("rustls client session")
    }

    /// Moonpool twin of the tokio engine's
    /// `driver::tests::cancelled_flush_leaves_the_write_arm_rearm_flag_set`.
    ///
    /// Both pin the same invariant: once a write round has left
    /// encrypted-but-unwritten bytes behind, the driver's `write_has_work`
    /// must still report work even though `pending_write` has drained —
    /// otherwise the write `select!` arm is gated off and nothing re-polls
    /// it. tokio cannot see rustls' residue through `tokio_rustls`, so it
    /// tracks it with an explicit `flush_pending` flag; moonpool owns the
    /// adapter and answers from real state via `has_pending_ciphertext()`,
    /// which is the extra term in its `write_has_work` (see `driver.rs`).
    #[test]
    fn absorbed_ciphertext_keeps_the_write_arm_armed() {
        let mut shared = TlsShared::new(RustlsByteAdapter::new(make_session()));
        assert!(
            !shared.has_pending_ciphertext(),
            "nothing has been absorbed yet"
        );

        // A fresh client session queues its ClientHello for the wire — the
        // stand-in for any encrypted-but-unwritten residue.
        shared.adapter.step().expect("adapter step");
        shared.absorb_adapter_output();

        assert!(
            shared.has_pending_ciphertext(),
            "absorbed ciphertext must keep the write arm armed; this is the \
             term the tokio engine has to emulate with `flush_pending`"
        );
    }

    /// A double whose `poll_write` always returns `Pending` and never
    /// registers a waker — models a kernel send buffer that isn't ready
    /// yet. `write_some_tls` issues AT MOST ONE `poll_write` per call, so
    /// this is enough to force it to suspend without ever accepting a
    /// single byte, letting the test prove a zero-progress cancellation is
    /// a true no-op (nothing committed, nothing sent).
    struct NeverReady;

    impl futures::io::AsyncWrite for NeverReady {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Accepts everything, every call — used to resume and finish the
    /// write after the cancelling drop.
    struct AcceptAll {
        accepted: std::sync::Arc<parking_lot::Mutex<Vec<u8>>>,
    }

    impl futures::io::AsyncWrite for AcceptAll {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.accepted.lock().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// `write_some_tls` performs AT MOST ONE low-level `poll_write` per
    /// call (the ADR-0083 cancellation-safety invariant), so there is no
    /// "partial write, then suspend" state to catch mid-function the way
    /// the tokio engine's multi-write *loop*
    /// (`PendingDriverWrite::write_budgeted`) has — a single poll either
    /// fully resolves (`Ready`, and the offset commits synchronously right
    /// there) or the whole call is `Pending` having committed nothing at
    /// all. This test pins exactly that: a cancelled (dropped) call that
    /// never got a `Ready` must leave `pending_ciphertext_offset` and the
    /// peer's received bytes both completely untouched, and a fresh call
    /// afterwards must deliver every byte exactly once.
    #[test]
    fn write_some_tls_is_cancel_safe_across_a_dropped_await() {
        use std::future::Future as _;

        let shared = Arc::new(parking_lot::Mutex::new(TlsShared::new(
            RustlsByteAdapter::new(make_session()),
        )));

        // Seed `pending_ciphertext` directly (bypassing the handshake —
        // this test is about the wire-drain bookkeeping, not rustls
        // protocol correctness, which `tls.rs` already covers). Encrypting
        // pre-handshake plaintext through the real adapter would just queue
        // a ClientHello, not our test payload, so we inject a synthetic
        // ciphertext-shaped payload directly.
        let source = b"the-quick-brown-fox-jumps-over-the-lazy-dog-tls".to_vec();
        {
            let mut g = shared.lock();
            g.pending_ciphertext.extend_from_slice(&source);
            g.pending_ciphertext_offset = 0;
        }

        let accepted = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut stalling = NeverReady;

        {
            let mut fut = std::pin::pin!(super::write_some_tls(&mut stalling, &shared, &[]));
            let waker = std::task::Waker::noop();
            let mut cx = Context::from_waker(waker);
            assert!(
                fut.as_mut().poll(&mut cx).is_pending(),
                "the double must force write_some_tls to suspend before any \
                 byte is accepted so dropping it below models a real, \
                 zero-progress select! cancel"
            );
            // `fut` is dropped here at the end of the block — the
            // cancellation.
        }

        assert_eq!(
            shared.lock().pending_ciphertext_offset,
            0,
            "a cancellation that never saw Ready must commit nothing — \
             offset must stay exactly where it was"
        );
        assert!(
            accepted.lock().is_empty(),
            "the never-ready double must not have received any bytes"
        );

        let mut resume = AcceptAll {
            accepted: accepted.clone(),
        };
        loop {
            let has_more = shared.lock().has_pending_ciphertext();
            if !has_more {
                break;
            }
            let mut fut = std::pin::pin!(super::write_some_tls(&mut resume, &shared, &[]));
            let waker = std::task::Waker::noop();
            let mut cx = Context::from_waker(waker);
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(_)) => {}
                other => panic!("resumed drain must complete synchronously: {other:?}"),
            }
        }

        assert!(!shared.lock().has_pending_ciphertext());
        assert_eq!(
            &accepted.lock()[..],
            &source[..],
            "the peer's total received bytes must equal the source exactly \
             once — no duplication from re-sending pre-cancellation bytes, \
             no gap from skipping them"
        );
    }
}
