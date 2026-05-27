// SPDX-License-Identifier: Apache-2.0

//! Token-based [`AuthProvider`] — bytes from string, env, or caller-supplied
//! closure.
//!
//! Mirrors `org.apache.pulsar.client.impl.auth.AuthenticationToken`. Three
//! constructors are exposed:
//!
//! - [`TokenAuth::from_string`] — bytes held inline.
//! - [`TokenAuth::from_env`] — bytes read at construction from an environment variable.
//! - [`TokenAuth::from_supplier`] — bytes re-computed by a caller-supplied closure on **every**
//!   [`AuthProvider::initial`] call, so on-disk token rotation is picked up without rebuilding the
//!   provider. File-backed convenience lives in the runtime crates (`magnetar-runtime-tokio`,
//!   `magnetar-runtime-moonpool`) where filesystem I/O is allowed (ADR-0004).
//!
//! The `method()` is always `"token"`.

use std::sync::Arc;

use bytes::Bytes;

use super::{AuthError, AuthProvider};

/// Caller-supplied closure that materialises the current token bytes. Invoked
/// on every [`AuthProvider::initial`] call, so token rotation can be plumbed
/// in without rebuilding the provider. The closure must be `Send + Sync`
/// because the surrounding [`crate::Connection`] is held behind a mutex that
/// is shared across runtime tasks.
pub type TokenSupplier = dyn Fn() -> Result<Bytes, AuthError> + Send + Sync;

/// Token-bearer auth provider.
#[derive(Clone)]
pub struct TokenAuth {
    source: TokenSource,
}

impl std::fmt::Debug for TokenAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match &self.source {
            TokenSource::Inline(_) => "Inline",
            TokenSource::Supplier(_) => "Supplier",
        };
        f.debug_struct("TokenAuth").field("source", &kind).finish()
    }
}

/// Backing storage for [`TokenAuth`].
#[derive(Clone)]
enum TokenSource {
    /// Token bytes held in-process.
    Inline(Bytes),
    /// Token bytes re-computed by a caller-supplied closure on every
    /// `initial()` call. File-backed rotation lives in the runtime crates.
    Supplier(Arc<TokenSupplier>),
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

    /// Construct a provider by reading the named environment variable at
    /// construction time.
    pub fn from_env(var: &str) -> Result<Self, AuthError> {
        let value = std::env::var(var)
            .map_err(|err| AuthError::Invalid(format!("env var {var}: {err}")))?;
        Ok(Self::from_string(value))
    }

    /// Construct a provider whose `initial()` calls invoke the supplied
    /// closure to materialise the current token bytes.
    ///
    /// Use this to plumb in token rotation (file-backed, network-backed,
    /// vault-backed, …) without leaking I/O into `magnetar-proto`. The
    /// runtime crates expose convenience wrappers — see
    /// `magnetar_runtime_tokio::file_token_auth(path)` for the disk case.
    #[must_use]
    pub fn from_supplier(supplier: Arc<TokenSupplier>) -> Self {
        Self {
            source: TokenSource::Supplier(supplier),
        }
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
            TokenSource::Supplier(f) => f(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;

    use super::{AuthError, AuthProvider, TokenAuth};

    #[test]
    fn from_string_round_trip() {
        let p = TokenAuth::from_string("hello");
        assert_eq!(p.method(), "token");
        assert_eq!(p.initial().expect("initial"), b"hello".as_slice());
    }

    #[test]
    fn from_env_reads_cargo_provided_var() {
        // `CARGO_PKG_NAME` is set by cargo for every build; we use it instead
        // of mutating the process env (forbidden under `forbid(unsafe_code)`
        // in edition 2024).
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
    fn from_supplier_invokes_closure_on_every_initial() {
        // The supplier closure is the rotation hook — `initial()` must call
        // it every time, not cache the first value. This is what file-backed
        // wrappers in the runtime crates rely on.
        let calls = Arc::new(AtomicUsize::new(0));
        let tokens: &[&[u8]] = &[b"first-token", b"second-token", b"third-token"];
        let calls_inner = calls.clone();
        let p = TokenAuth::from_supplier(Arc::new(move || {
            let n = calls_inner.fetch_add(1, Ordering::SeqCst);
            Ok(Bytes::copy_from_slice(tokens[n.min(tokens.len() - 1)]))
        }));

        assert_eq!(p.method(), "token");
        assert_eq!(p.initial().expect("initial"), b"first-token".as_slice());
        assert_eq!(p.initial().expect("initial"), b"second-token".as_slice());
        assert_eq!(p.initial().expect("initial"), b"third-token".as_slice());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn from_supplier_propagates_errors() {
        let p = TokenAuth::from_supplier(Arc::new(|| {
            Err(AuthError::Io("synthetic supplier failure".to_owned()))
        }));
        let err = p.initial().unwrap_err();
        assert!(
            err.to_string().contains("synthetic supplier failure"),
            "err={err}",
        );
    }
}
