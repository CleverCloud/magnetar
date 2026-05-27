// SPDX-License-Identifier: Apache-2.0

//! [`Transmit`] — the sans-io ↔ runtime outbound-byte descriptor.
//!
//! ADR-0039 wave 1.0: introduces the [`Transmit`] enum and a
//! `poll_transmit_vectored` entry point on [`crate::Connection`]
//! returning the enum, without changing the legacy
//! [`crate::Connection::poll_transmit`] signature yet.
//! Today the [`Transmit::Vectored`] variant exists in the type but is
//! never produced — the wave-1.0 implementation always returns
//! [`Transmit::Contiguous`] (semantically equivalent to the legacy
//! path). Wave 1.1 (the proto encoder split) will start emitting
//! `Vectored` for producer batches; wave 2 (moonpool
//! `Providers::Network::write_vectored`) makes the chaos pack
//! segment-aware. See
//! [ADR-0039](../../specs/adr/0039-vectored-io-transmit-enum.md).

use bytes::Bytes;

/// Outbound-byte descriptor returned by
/// [`crate::Connection::poll_transmit_vectored`]. Runtimes that
/// support vectored writes can dispatch the [`Self::Vectored`] variant
/// via `poll_write_vectored` / `IoSlice` to avoid the user-space
/// memcpy that the contiguous-coalesce path incurs.
///
/// Runtimes that do not (yet) support vectored writes coalesce
/// `Vectored` segments into a single buffer themselves — semantically
/// equivalent to the legacy [`crate::Connection::poll_transmit`]
/// `Bytes` return.
#[derive(Debug, Clone)]
pub enum Transmit<'a> {
    /// Single contiguous slice — used by TLS (rustls coalesces
    /// internally so segment fidelity is wasted), small handshake
    /// frames, and any path the protocol layer can't trivially split.
    Contiguous(&'a [u8]),
    /// Segment list — used by producer batches in plaintext mode.
    /// Each [`Bytes`] carries one frame head or payload. The runtime
    /// passes the list through `poll_write_vectored` /
    /// `Providers::Network::write_vectored`. Empty list is permitted
    /// (no outbound bytes; equivalent to `Contiguous(&[])`).
    Vectored(&'a [Bytes]),
}

impl<'a> Transmit<'a> {
    /// Total byte count across all segments. `Contiguous(&buf).len()`
    /// for the contiguous variant; sum of segment lengths for vectored.
    /// Used by the runtime to short-circuit empty transmits and to
    /// budget the next `poll_write` call.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Contiguous(buf) => buf.len(),
            Self::Vectored(segs) => segs.iter().map(Bytes::len).sum(),
        }
    }

    /// `true` if there is nothing to transmit.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contiguous_len_matches_slice() {
        let buf = b"hello world";
        let t = Transmit::Contiguous(buf);
        assert_eq!(t.len(), 11);
        assert!(!t.is_empty());
    }

    #[test]
    fn vectored_len_sums_segments() {
        let segs: Vec<Bytes> = vec![
            Bytes::from_static(b"hello"),
            Bytes::from_static(b" "),
            Bytes::from_static(b"world"),
        ];
        let t = Transmit::Vectored(&segs);
        assert_eq!(t.len(), 11);
        assert!(!t.is_empty());
    }

    #[test]
    fn empty_contiguous_is_empty() {
        let t = Transmit::Contiguous(&[]);
        assert_eq!(t.len(), 0);
        assert!(t.is_empty());
    }

    #[test]
    fn empty_vectored_is_empty() {
        let segs: Vec<Bytes> = Vec::new();
        let t = Transmit::Vectored(&segs);
        assert_eq!(t.len(), 0);
        assert!(t.is_empty());
    }

    #[test]
    fn vectored_with_zero_length_segments_is_empty() {
        let segs: Vec<Bytes> = vec![Bytes::new(), Bytes::new()];
        let t = Transmit::Vectored(&segs);
        assert_eq!(t.len(), 0);
        assert!(t.is_empty());
    }
}
