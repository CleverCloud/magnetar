// SPDX-License-Identifier: Apache-2.0

//! rustls-over-bytepipe TLS adapter.
//!
//! `rustls::ClientConnection` is itself sans-io: it offers `read_tls`,
//! `process_new_packets`, `write_tls`, `wants_read`, `wants_write`. This
//! adapter drives those methods against an arbitrary byte pipe (in
//! particular, a moonpool-supplied `AsyncRead + AsyncWrite` stream),
//! making TLS handshakes deterministic under `moonpool-sim` chaos
//! testing — option (d) from
//! [`docs/decisions-log.md`](../../docs/decisions-log.md), atomised as
//! [ADR-0006](../../specs/adr/0006-moonpool-tls-byte-pipe.md).
//! See also [ADR-0005](../../specs/adr/0005-rustls-only-tls.md) for the
//! workspace-wide ban on `native-tls` / `openssl`.
//!
//! Usage shape:
//!
//! ```ignore
//! let session = rustls::ClientConnection::new(config, server_name)?;
//! let mut adapter = RustlsByteAdapter::new(session);
//!
//! // From the moonpool engine driver loop, on each iteration:
//! adapter.push_encrypted(&from_wire);
//! adapter.step()?;
//! let plaintext = adapter.take_plaintext();
//! // ... feed plaintext into magnetar_proto::Connection::handle_bytes ...
//!
//! // Going the other way:
//! adapter.push_plaintext(&from_magnetar_proto);
//! adapter.step()?;
//! let encrypted_to_wire = adapter.take_encrypted_outbound();
//! ```

use std::io::{self, Cursor, Read, Write};

use bytes::{Bytes, BytesMut};
use rustls::ClientConnection;

/// Sans-io adapter pairing [`rustls::ClientConnection`] with magnetar's
/// byte-pipe-shaped engine.
pub struct RustlsByteAdapter {
    session: ClientConnection,
    /// Encrypted bytes received from the wire, fed into rustls.
    inbox_encrypted: BytesMut,
    /// Plaintext bytes ready to be consumed by magnetar-proto.
    inbox_plaintext: BytesMut,
    /// Plaintext bytes from magnetar-proto, waiting to be encrypted.
    outbox_plaintext: BytesMut,
    /// Encrypted bytes ready to be sent on the wire.
    outbox_encrypted: BytesMut,
}

impl std::fmt::Debug for RustlsByteAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustlsByteAdapter")
            .field("inbox_encrypted_len", &self.inbox_encrypted.len())
            .field("inbox_plaintext_len", &self.inbox_plaintext.len())
            .field("outbox_plaintext_len", &self.outbox_plaintext.len())
            .field("outbox_encrypted_len", &self.outbox_encrypted.len())
            .field("is_handshaking", &self.session.is_handshaking())
            .field("wants_read", &self.session.wants_read())
            .field("wants_write", &self.session.wants_write())
            .finish()
    }
}

impl RustlsByteAdapter {
    /// Wrap a `ClientConnection` in the adapter.
    #[must_use]
    pub fn new(session: ClientConnection) -> Self {
        Self {
            session,
            inbox_encrypted: BytesMut::with_capacity(16 * 1024),
            inbox_plaintext: BytesMut::with_capacity(16 * 1024),
            outbox_plaintext: BytesMut::with_capacity(16 * 1024),
            outbox_encrypted: BytesMut::with_capacity(16 * 1024),
        }
    }

    /// True when the TLS handshake is still in progress.
    #[must_use]
    pub fn is_handshaking(&self) -> bool {
        self.session.is_handshaking()
    }

    /// Push encrypted bytes received from the wire into the adapter. Call
    /// [`Self::step`] afterwards to advance the TLS state machine.
    pub fn push_encrypted(&mut self, bytes: &[u8]) {
        self.inbox_encrypted.extend_from_slice(bytes);
    }

    /// Push plaintext bytes (e.g. from `Connection::poll_transmit`) into the
    /// adapter. Call [`Self::step`] afterwards.
    pub fn push_plaintext(&mut self, bytes: &[u8]) {
        self.outbox_plaintext.extend_from_slice(bytes);
    }

    /// Drive the TLS state machine: consume queued encrypted bytes via
    /// `read_tls`, decrypt via `process_new_packets`, drain plaintext into
    /// `inbox_plaintext`, then take queued plaintext via the session's
    /// writer and let `write_tls` produce encrypted bytes for the wire.
    ///
    /// # Errors
    /// Returns [`rustls::Error`] if the TLS state machine rejects input
    /// (bad cert, version mismatch, decrypt failure, etc.).
    pub fn step(&mut self) -> Result<(), rustls::Error> {
        // 1. Push encrypted bytes from inbox_encrypted into the session.
        if !self.inbox_encrypted.is_empty() {
            let mut cursor = Cursor::new(self.inbox_encrypted.as_ref());
            // read_tls consumes whatever it can; returns bytes_consumed.
            let consumed = self
                .session
                .read_tls(&mut cursor)
                .map_err(|err| rustls::Error::General(format!("read_tls: {err}")))?;
            // Drop the consumed prefix.
            let _ = self.inbox_encrypted.split_to(consumed);
            // Now run the state machine.
            let _state = self.session.process_new_packets()?;
        }

        // 2. Drain decrypted plaintext into inbox_plaintext. The session's `reader()` yields
        //    decrypted application data.
        let mut buf = [0u8; 8192];
        loop {
            match self.session.reader().read(&mut buf) {
                Ok(0) => break,
                Ok(n) => self.inbox_plaintext.extend_from_slice(&buf[..n]),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }

        // 3. Push outbox_plaintext into the session for encryption.
        if !self.outbox_plaintext.is_empty() {
            // The session writer doesn't fail on capacity (per rustls docs);
            // ignore Result and drop any unwritten bytes by truncating.
            let written = self
                .session
                .writer()
                .write(self.outbox_plaintext.as_ref())
                .unwrap_or(0);
            let _ = self.outbox_plaintext.split_to(written);
        }

        // 4. Drain encrypted bytes from the session into outbox_encrypted.
        let mut sink = Vec::with_capacity(8192);
        let _ = self
            .session
            .write_tls(&mut sink)
            .map_err(|err| rustls::Error::General(format!("write_tls: {err}")))?;
        self.outbox_encrypted.extend_from_slice(&sink);
        Ok(())
    }

    /// Take decrypted plaintext ready for `magnetar_proto::Connection::handle_bytes`.
    /// After calling, the internal buffer is empty.
    #[must_use]
    pub fn take_plaintext(&mut self) -> Bytes {
        self.inbox_plaintext.split().freeze()
    }

    /// Take encrypted bytes ready for the wire. After calling, the internal
    /// buffer is empty.
    #[must_use]
    pub fn take_encrypted_outbound(&mut self) -> Bytes {
        self.outbox_encrypted.split().freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::RustlsByteAdapter;

    /// Build an `Arc<ClientConfig>` from an empty (intentionally invalid) trust
    /// store. The `ClientConnection` will still construct; we only smoke-test
    /// that the adapter's buffer accounting works. The rustls crypto provider
    /// is picked by the workspace's `crypto-*` feature (issue #9, ADR-0035)
    /// via the explicit [`crate::tls_crypto::active_provider`] shim.
    fn make_session() -> rustls::ClientConnection {
        // With both `ring` and `aws-lc-rs` features active in the dep graph
        // (reqwest 0.13 pulls `rustls-platform-verifier` which enables
        // `aws-lc-rs`), rustls no longer auto-selects a provider. Install
        // the `ring` provider for the test process; idempotent across tests.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let root_store = rustls::RootCertStore::empty();
        let config = std::sync::Arc::new(
            rustls::ClientConfig::builder_with_provider(crate::tls_crypto::active_provider())
                .with_safe_default_protocol_versions()
                .expect("rustls default protocol versions are valid")
                .with_root_certificates(root_store)
                .with_no_client_auth(),
        );
        let name = rustls::pki_types::ServerName::try_from("example.com").unwrap();
        rustls::ClientConnection::new(config, name).expect("rustls client session")
    }

    #[test]
    fn adapter_compiles_and_starts_handshaking() {
        let session = make_session();
        let mut adapter = RustlsByteAdapter::new(session);
        assert!(adapter.is_handshaking());
        // Pushing zero encrypted bytes should be a no-op.
        adapter.push_encrypted(&[]);
        adapter.step().unwrap();
        // The client should have queued ClientHello bytes for the wire.
        let outbound = adapter.take_encrypted_outbound();
        assert!(
            !outbound.is_empty(),
            "client should have produced ClientHello bytes"
        );
    }

    #[test]
    fn plaintext_push_and_take_round_trip() {
        let session = make_session();
        let mut adapter = RustlsByteAdapter::new(session);
        // Before handshake completes, plaintext writes accumulate but won't
        // surface decrypted bytes — we just verify buffer accounting.
        adapter.push_plaintext(b"hello");
        adapter.step().unwrap();
        let taken = adapter.take_plaintext();
        assert!(
            taken.is_empty(),
            "no decrypted plaintext should appear pre-handshake"
        );
    }

    /// Push enough garbage encrypted bytes to drive rustls past the
    /// ClientHello / fatal-alert split. Confirms `step()` surfaces the
    /// rustls error path rather than silently dropping bytes — important
    /// because the moonpool transport relies on it to terminate `read_buf`
    /// with `InvalidData` rather than hang.
    #[test]
    fn adapter_step_propagates_decrypt_error() {
        let session = make_session();
        let mut adapter = RustlsByteAdapter::new(session);
        // Issue the ClientHello first so rustls is past the "waiting for
        // input" stage. The subsequent garbage push fails decryption.
        adapter.step().unwrap();
        let _ = adapter.take_encrypted_outbound();
        // Push a TLS-record-header-shaped payload that decrypts to nothing
        // useful. rustls should reject it on `process_new_packets`.
        let bogus = vec![0x17, 0x03, 0x03, 0x00, 0x05, 0xff, 0xff, 0xff, 0xff, 0xff];
        adapter.push_encrypted(&bogus);
        let outcome = adapter.step();
        assert!(
            outcome.is_err(),
            "rustls must reject the bogus record, got {outcome:?}"
        );
    }
}
