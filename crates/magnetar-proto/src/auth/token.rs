// SPDX-License-Identifier: Apache-2.0

//! Token-based [`AuthProvider`] — bytes from string, env, or file.
//!
//! Mirrors `org.apache.pulsar.client.impl.auth.AuthenticationToken`. Three constructors are
//! exposed:
//!
//! - [`TokenAuth::from_string`] — bytes held inline.
//! - [`TokenAuth::from_env`] — bytes read at construction from an environment variable.
//! - [`TokenAuth::from_file`] — bytes re-read from a file on **every** [`AuthProvider::initial`]
//!   call, so on-disk token rotation is picked up without rebuilding the provider.
//!
//! The `method()` is always `"token"`.

use std::fs;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::{AuthError, AuthProvider};

/// Token-bearer auth provider.
#[derive(Debug, Clone)]
pub struct TokenAuth {
    source: TokenSource,
}

/// Backing storage for [`TokenAuth`].
#[derive(Debug, Clone)]
enum TokenSource {
    /// Token bytes held in-process.
    Inline(Bytes),
    /// Token bytes that should be re-read from disk on every `initial()` call.
    File(PathBuf),
}

impl TokenAuth {
    /// Construct a provider from a literal token string.
    #[must_use]
    pub fn from_string(token: impl Into<String>) -> Self {
        Self {
            source: TokenSource::Inline(Bytes::from(token.into())),
        }
    }

    /// Construct a provider from raw bytes (e.g. when the token came over a control plane).
    #[must_use]
    pub fn from_bytes(token: Bytes) -> Self {
        Self {
            source: TokenSource::Inline(token),
        }
    }

    /// Construct a provider by reading the named environment variable at construction time.
    pub fn from_env(var: &str) -> Result<Self, AuthError> {
        let value = std::env::var(var)
            .map_err(|err| AuthError::Invalid(format!("env var {var}: {err}")))?;
        Ok(Self::from_string(value))
    }

    /// Construct a provider that re-reads its token file on every `initial()` call.
    ///
    /// The file is read eagerly at construction once to validate the path exists; subsequent
    /// `initial()` calls re-read it so that token rotation works without restarting.
    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, AuthError> {
        let path = path.into();
        // Eager validation: surface a clear error at construction if the path is broken.
        Self::read_token_file(&path)?;
        Ok(Self {
            source: TokenSource::File(path),
        })
    }

    fn read_token_file(path: &Path) -> Result<Bytes, AuthError> {
        let raw = fs::read(path).map_err(|err| {
            AuthError::Io(format!("reading token file {}: {err}", path.display()))
        })?;
        // Strip a single trailing newline if present — the Java client trims tokens read from
        // disk.
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
}

impl AuthProvider for TokenAuth {
    #[allow(clippy::unnecessary_literal_bound)]
    fn method(&self) -> &str {
        "token"
    }

    fn initial(&self) -> Result<Bytes, AuthError> {
        match &self.source {
            TokenSource::Inline(b) => Ok(b.clone()),
            TokenSource::File(path) => Self::read_token_file(path),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::{AuthProvider, TokenAuth};

    #[test]
    fn from_string_round_trip() {
        let p = TokenAuth::from_string("hello");
        assert_eq!(p.method(), "token");
        assert_eq!(p.initial().expect("initial"), b"hello".as_slice());
    }

    #[test]
    fn from_env_reads_cargo_provided_var() {
        // `CARGO_PKG_NAME` is set by cargo for every build; we use it instead of mutating the
        // process env (forbidden under `forbid(unsafe_code)` in edition 2024).
        let p = TokenAuth::from_env("CARGO_PKG_NAME").expect("env var");
        assert_eq!(p.initial().expect("initial"), b"magnetar-proto".as_slice());
    }

    #[test]
    fn from_env_missing() {
        // A clearly-not-set variable name should surface an `Invalid` error.
        let err = TokenAuth::from_env("MAGNETAR_DEFINITELY_NOT_SET_XYZZY_42").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("MAGNETAR_DEFINITELY_NOT_SET_XYZZY_42"),
            "msg={msg}"
        );
    }

    #[test]
    fn from_file_round_trip_and_rotation() {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        writeln!(file, "first-token").expect("write");
        file.flush().expect("flush");
        let path = file.path().to_owned();

        let p = TokenAuth::from_file(&path).expect("from_file");
        assert_eq!(p.method(), "token");
        assert_eq!(p.initial().expect("initial"), b"first-token".as_slice());

        // Rotate the file in-place and re-read.
        std::fs::write(&path, b"second-token").expect("rewrite");
        assert_eq!(
            p.initial().expect("initial after rotation"),
            b"second-token".as_slice()
        );
    }

    #[test]
    fn from_file_missing_path() {
        let err = TokenAuth::from_file("/this/path/does/not/exist/magnetar-token").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("token file"), "msg={msg}");
    }
}
