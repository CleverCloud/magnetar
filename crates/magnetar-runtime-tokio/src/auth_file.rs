// SPDX-License-Identifier: Apache-2.0

//! Filesystem-backed convenience wrappers for proto-layer auth providers.
//!
//! `magnetar-proto` keeps the auth providers I/O-free per ADR-0004; the disk
//! I/O for file-backed token rotation lives here in the tokio runtime crate
//! where `std::fs` is allowed.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use magnetar_proto::{AuthError, TokenAuth, TokenSupplier};

/// Read a token from `path` and strip a single trailing `\r?\n`. Mirrors the
/// Java client's `AuthenticationToken#readTokenFromFile` byte-trimming
/// contract (Pulsar legacy convention: tokens written by config-management
/// tooling tend to carry a terminal newline).
fn read_token_from_disk(path: &Path) -> Result<Bytes, AuthError> {
    let raw = std::fs::read(path)
        .map_err(|err| AuthError::Io(format!("reading token file {}: {err}", path.display())))?;
    let trimmed = match raw.last() {
        Some(b'\n') => &raw[..raw.len() - 1],
        _ => &raw[..],
    };
    let trimmed = match trimmed.last() {
        Some(b'\r') => &trimmed[..trimmed.len() - 1],
        _ => trimmed,
    };
    Ok(Bytes::copy_from_slice(trimmed))
}

/// Build a [`TokenAuth`] that re-reads `path` on every
/// [`magnetar_proto::AuthProvider::initial`] call — the canonical
/// file-backed token rotation pattern.
///
/// `path` is validated eagerly at construction: a missing or unreadable file
/// surfaces an [`AuthError::Io`] immediately so the broker handshake does not
/// have to be the first thing that learns about a typo'd config.
///
/// # Errors
///
/// Returns [`AuthError::Io`] when the initial read fails (path missing,
/// unreadable, etc.). Subsequent failures on rotation are surfaced through
/// `AuthProvider::initial`.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use magnetar_runtime_tokio::file_token_auth;
///
/// let auth = file_token_auth("/var/run/secrets/pulsar.token")
///     .expect("token file readable");
/// let provider: Arc<dyn magnetar_proto::AuthProvider> = Arc::new(auth);
/// ```
pub fn file_token_auth(path: impl Into<PathBuf>) -> Result<TokenAuth, AuthError> {
    let path: PathBuf = path.into();
    // Eager validation: surface a clear error at construction if the path is
    // broken. Mirrors the original `TokenAuth::from_file` contract before
    // the proto-layer `fs::read` was moved out into this runtime crate.
    read_token_from_disk(&path)?;
    let supplier: Arc<TokenSupplier> = Arc::new(move || read_token_from_disk(&path));
    Ok(TokenAuth::from_supplier(supplier))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use magnetar_proto::AuthProvider;

    use super::file_token_auth;

    #[test]
    fn round_trip_and_rotation() {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        writeln!(file, "first-token").expect("write");
        file.flush().expect("flush");
        let path = file.path().to_owned();

        let p = file_token_auth(&path).expect("file_token_auth");
        assert_eq!(p.method(), "token");
        assert_eq!(p.initial().expect("initial"), b"first-token".as_slice());

        std::fs::write(&path, b"second-token").expect("rewrite");
        assert_eq!(
            p.initial().expect("initial after rotation"),
            b"second-token".as_slice()
        );
    }

    #[test]
    fn missing_path_surfaces_io_error() {
        let err = file_token_auth("/this/path/does/not/exist/magnetar-token").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("token file"), "msg={msg}");
    }
}
