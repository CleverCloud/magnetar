// SPDX-License-Identifier: Apache-2.0

//! Transport layer for the moonpool engine.
//!
//! Wraps a [`moonpool_core::NetworkProvider::TcpStream`] behind a thin
//! adapter that exposes the read/write surface the driver loop needs. The
//! underlying stream is already `AsyncRead + AsyncWrite + Unpin`, so this
//! adapter is little more than a typed alias — the value is in keeping the
//! engine generic over `P: Providers` without leaking `tokio::net::TcpStream`
//! everywhere.
//!
//! TLS is handled in [`crate::tls::RustlsByteAdapter`] and composed into the
//! driver in a later milestone; this module owns the plaintext byte pipe
//! only.

use std::io;

use moonpool_core::{NetworkProvider, Providers};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::EngineError;

/// A plaintext TCP stream produced by the configured
/// [`moonpool_core::Providers`]. Owned by the driver task — one transport per
/// connection, never shared.
pub(crate) struct Transport<P: Providers> {
    stream: <P::Network as NetworkProvider>::TcpStream,
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
        Ok(Self { stream })
    }

    /// Wrap an already-established stream (used by tests and by the future
    /// TLS path where the TCP socket is set up separately).
    #[cfg(test)]
    #[allow(dead_code, reason = "wired by M3 TLS adapter; kept for symmetry today")]
    pub(crate) fn from_stream(stream: <P::Network as NetworkProvider>::TcpStream) -> Self {
        Self { stream }
    }

    /// Read up to `buf.len()` bytes from the wire. Returns `0` on a clean EOF.
    ///
    /// Provided for symmetry with the documented transport surface and for
    /// the TLS adapter path (M3), which reads into a fixed-size buffer
    /// before handing bytes to [`crate::tls::RustlsByteAdapter`]. The plain
    /// driver uses [`Self::read_buf`] instead.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncRead::poll_read` error.
    #[allow(dead_code)] // Used by the TLS adapter path in a follow-up milestone.
    pub(crate) async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf).await
    }

    /// Read into a [`bytes::BytesMut`] using
    /// [`tokio::io::AsyncReadExt::read_buf`]. Returns `0` on a clean EOF.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncRead::poll_read` error.
    pub(crate) async fn read_buf(&mut self, buf: &mut bytes::BytesMut) -> io::Result<usize> {
        self.stream.read_buf(buf).await
    }

    /// Write the entire `buf` to the wire, looping over short writes.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_write` error.
    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.stream.write_all(buf).await
    }

    /// Flush any buffered bytes.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_flush` error.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().await
    }

    /// Shut the stream down cleanly. Errors here are non-fatal (the driver
    /// only attempts a shutdown on the happy path), so callers typically
    /// `let _ = transport.shutdown().await;`.
    ///
    /// # Errors
    /// Propagates the underlying `AsyncWrite::poll_shutdown` error.
    pub(crate) async fn shutdown(&mut self) -> io::Result<()> {
        self.stream.shutdown().await
    }
}

impl<P: Providers> std::fmt::Debug for Transport<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport").finish_non_exhaustive()
    }
}
