// SPDX-License-Identifier: Apache-2.0

//! Sans-io authentication provider trait + `AUTH_CHALLENGE` helper.
//!
//! The `Connection` state machine has no I/O of its own; authentication providers must therefore
//! be synchronous and side-effect-free at the trait surface. Concrete providers are free to
//! cache, refresh, or read filesystem state inside their own implementation as long as the trait
//! method itself produces deterministic `Bytes` for a given internal state.
//!
//! Two provider kinds ship in this crate:
//!
//! - [`TokenAuth`] — token-based bearer auth, from string / env / file.
//! - [`TlsAuth`] — mTLS handshake material (the cert + key are surfaced; the actual TLS handshake
//!   happens in the runtime engine).
//!
//! Higher-level providers (OAuth2, SASL, Athenz) live in their own sub-crates so they may pull in
//! their own dependencies without polluting `magnetar-proto`'s zero-I/O dep graph.
//!
//! # Reference
//!
//! Mirrors the shape of `org.apache.pulsar.client.api.Authentication` (`pulsar-client-api/src/
//! main/java/org/apache/pulsar/client/api/Authentication.java`) and the `getAuthData()` /
//! `authenticationStage()` plumbing in `ClientCnx.handleAuthChallenge` (`pulsar-client/src/main/
//! java/org/apache/pulsar/client/impl/ClientCnx.java:464-518`).

use core::fmt;
use std::error::Error as StdError;

use bytes::Bytes;

use crate::pb;

pub mod tls;
pub mod token;

pub use tls::TlsAuth;
pub use token::TokenAuth;

/// Auth provider error surface.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// The provider was asked to operate on invalid state (e.g. a malformed token).
    #[error("invalid auth state: {0}")]
    Invalid(String),

    /// The credentials have expired and cannot be refreshed by this provider.
    #[error("auth credentials expired: {0}")]
    Expired(String),

    /// The requested feature is not implemented by this provider yet.
    #[error("unsupported auth operation: {0}")]
    Unsupported(String),

    /// The on-disk material backing the provider could not be read.
    #[error("auth material read failed: {0}")]
    Io(String),

    /// A downstream provider-specific error.
    #[error("auth provider error: {0}")]
    Provider(#[source] Box<dyn StdError + Send + Sync + 'static>),
}

/// Synchronous, sans-io authentication provider.
///
/// Concrete impls live in:
/// - [`TokenAuth`] (in-tree, this crate)
/// - [`TlsAuth`] (in-tree, this crate)
/// - `magnetar-auth-oauth2::OAuth2Provider`
/// - `magnetar-auth-sasl::SaslPlain` (and the optional Kerberos surface)
/// - `magnetar-auth-athenz::AthenzProvider` (stub for M6 — surfaces `Unsupported`)
pub trait AuthProvider: Send + Sync + fmt::Debug {
    /// The Pulsar `auth_method_name` (e.g. `"token"`, `"tls"`, `"oauth2"`).
    fn method(&self) -> &str;

    /// Bytes to populate in the initial `CommandConnect.auth_data`.
    ///
    /// Providers that derive their material from external state (e.g. a token file) must re-read
    /// that state here so that rotation works without reconstructing the provider.
    fn initial(&self) -> Result<Bytes, AuthError>;

    /// Bytes to populate in the `CommandAuthResponse` after the broker issued a
    /// `CommandAuthChallenge`.
    ///
    /// The default implementation simply re-invokes [`AuthProvider::initial`], matching the
    /// "refresh-and-resend" semantics of the Pulsar Java client. Providers that participate in a
    /// real multi-step handshake (SASL, GSSAPI) override this to consume the server challenge.
    fn respond_to_challenge(&self, _challenge: &[u8]) -> Result<Bytes, AuthError> {
        self.initial()
    }
}

/// Tracks whether the connection is currently in the middle of an `AUTH_CHALLENGE` exchange.
///
/// The `Connection` state machine owns one instance; helpers route a server-issued
/// [`pb::CommandAuthChallenge`] through [`AuthChallengeState::handle_challenge`] to produce the
/// corresponding [`pb::CommandAuthResponse`].
#[derive(Debug, Default)]
pub struct AuthChallengeState {
    in_progress: bool,
    /// Number of completed challenge-response round-trips on this connection.
    completed: u32,
}

impl AuthChallengeState {
    /// Construct an idle challenge tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` while the connection is between receiving an `AUTH_CHALLENGE` and emitting the
    /// matching `CommandAuthResponse`.
    #[must_use]
    pub fn is_in_progress(&self) -> bool {
        self.in_progress
    }

    /// Number of completed challenge-response round-trips since construction.
    #[must_use]
    pub fn completed(&self) -> u32 {
        self.completed
    }

    /// Consume a server-issued [`pb::CommandAuthChallenge`] and produce the matching
    /// [`pb::CommandAuthResponse`] by interrogating the configured [`AuthProvider`].
    ///
    /// The provider's [`AuthProvider::respond_to_challenge`] is invoked with the broker's
    /// `auth_data` bytes (empty slice if the broker omitted them).
    pub fn handle_challenge(
        &mut self,
        cmd: &pb::CommandAuthChallenge,
        provider: &dyn AuthProvider,
    ) -> Result<pb::CommandAuthResponse, AuthError> {
        self.in_progress = true;
        let challenge_bytes: &[u8] = cmd
            .challenge
            .as_ref()
            .and_then(|d| d.auth_data.as_deref())
            .unwrap_or(&[]);
        let response = provider.respond_to_challenge(challenge_bytes)?;
        let out = pb::CommandAuthResponse {
            client_version: None,
            protocol_version: cmd.protocol_version,
            response: Some(pb::AuthData {
                auth_method_name: Some(provider.method().to_owned()),
                auth_data: Some(response.to_vec()),
            }),
        };
        self.in_progress = false;
        self.completed = self.completed.saturating_add(1);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{AuthChallengeState, AuthError, AuthProvider};
    use crate::pb;

    /// Hard-coded provider used to validate `AuthChallengeState`.
    #[derive(Debug)]
    struct StaticAuthProvider {
        method: &'static str,
        initial: Bytes,
        response: Bytes,
    }

    impl AuthProvider for StaticAuthProvider {
        fn method(&self) -> &str {
            self.method
        }
        fn initial(&self) -> Result<Bytes, AuthError> {
            Ok(self.initial.clone())
        }
        fn respond_to_challenge(&self, _challenge: &[u8]) -> Result<Bytes, AuthError> {
            Ok(self.response.clone())
        }
    }

    #[test]
    fn challenge_state_starts_idle() {
        let state = AuthChallengeState::new();
        assert!(!state.is_in_progress());
        assert_eq!(state.completed(), 0);
    }

    #[test]
    fn handle_challenge_uses_response_bytes() {
        let provider = StaticAuthProvider {
            method: "token",
            initial: Bytes::from_static(b"initial"),
            response: Bytes::from_static(b"refreshed"),
        };
        let mut state = AuthChallengeState::new();
        let cmd = pb::CommandAuthChallenge {
            server_version: Some("test/0".to_owned()),
            challenge: Some(pb::AuthData {
                auth_method_name: Some("token".to_owned()),
                auth_data: Some(b"server-nonce".to_vec()),
            }),
            protocol_version: Some(21),
        };
        let response = state
            .handle_challenge(&cmd, &provider)
            .expect("challenge response");
        assert_eq!(state.completed(), 1);
        assert!(!state.is_in_progress());
        let inner = response.response.expect("response payload");
        assert_eq!(inner.auth_method_name.as_deref(), Some("token"));
        assert_eq!(inner.auth_data.as_deref(), Some(b"refreshed".as_slice()));
        assert_eq!(response.protocol_version, Some(21));
    }

    /// Provider that falls back to the default `respond_to_challenge` (re-invokes `initial`).
    #[derive(Debug)]
    struct EchoProvider {
        bytes: Bytes,
    }

    impl AuthProvider for EchoProvider {
        #[allow(clippy::unnecessary_literal_bound)]
        fn method(&self) -> &str {
            "echo"
        }
        fn initial(&self) -> Result<Bytes, AuthError> {
            Ok(self.bytes.clone())
        }
    }

    #[test]
    fn default_respond_invokes_initial() {
        let provider = EchoProvider {
            bytes: Bytes::from_static(b"static-token"),
        };
        let mut state = AuthChallengeState::new();
        let cmd = pb::CommandAuthChallenge {
            server_version: None,
            challenge: None,
            protocol_version: None,
        };
        let response = state
            .handle_challenge(&cmd, &provider)
            .expect("challenge response");
        let inner = response.response.expect("response payload");
        assert_eq!(inner.auth_method_name.as_deref(), Some("echo"));
        assert_eq!(inner.auth_data.as_deref(), Some(b"static-token".as_slice()));
        assert_eq!(state.completed(), 1);
    }

    /// Provider that scripts a multi-step SASL Kerberos / GSSAPI exchange.
    /// Models what `magnetar_auth_sasl::SaslKerberos` does at the trait
    /// surface: the server-issued challenge is fed into a step counter, and
    /// each response carries a distinct continuation token. Pinning this in
    /// `magnetar-proto` (instead of in the auth crate) ensures the sans-io
    /// state machine itself handles arbitrary multi-round SASL handshakes —
    /// not just the single-round token-refresh case (PIP-30 / PIP-292).
    #[derive(Debug)]
    struct ScriptedSaslProvider {
        replies: std::sync::Mutex<std::vec::IntoIter<Bytes>>,
        /// Last challenge bytes observed — assertions in the test read this.
        last_challenge: std::sync::Mutex<Option<Vec<u8>>>,
    }

    impl ScriptedSaslProvider {
        fn new(replies: Vec<Bytes>) -> Self {
            Self {
                replies: std::sync::Mutex::new(replies.into_iter()),
                last_challenge: std::sync::Mutex::new(None),
            }
        }
    }

    impl AuthProvider for ScriptedSaslProvider {
        fn method(&self) -> &str {
            "sasl"
        }
        fn initial(&self) -> Result<Bytes, AuthError> {
            // The very first call carries an empty challenge — record it for
            // the assertions in the multi-round test below.
            *self.last_challenge.lock().expect("mutex poisoned") = Some(Vec::new());
            self.replies
                .lock()
                .expect("mutex poisoned")
                .next()
                .ok_or_else(|| AuthError::Invalid("scripted SASL transcript exhausted".to_owned()))
        }
        fn respond_to_challenge(&self, challenge: &[u8]) -> Result<Bytes, AuthError> {
            *self.last_challenge.lock().expect("mutex poisoned") = Some(challenge.to_vec());
            self.replies
                .lock()
                .expect("mutex poisoned")
                .next()
                .ok_or_else(|| AuthError::Invalid("scripted SASL transcript exhausted".to_owned()))
        }
    }

    /// Multi-round SASL Kerberos handshake: the broker issues three
    /// `CommandAuthChallenge` events in sequence, and the connection state
    /// must thread each one back into the same `AuthProvider` instance via
    /// `respond_to_challenge`. Validates that:
    /// 1. `AuthChallengeState` is reusable across consecutive challenges.
    /// 2. `completed()` reports the cumulative round-trip count.
    /// 3. The protocol-version echo on every `CommandAuthResponse` matches the broker's
    ///    `CommandAuthChallenge.protocol_version`.
    /// 4. The `auth_method_name` is constant across the whole exchange (the SASL mechanism doesn't
    ///    change mid-handshake).
    ///
    /// Mirrors the GSSAPI initiate-loop the Java `AuthenticationSasl`
    /// client drives over `ClientCnx.handleAuthChallenge` until SASL state
    /// flips to COMPLETE.
    #[test]
    fn multi_round_handshake_threads_continuation_tokens() {
        let provider = ScriptedSaslProvider::new(vec![
            Bytes::from_static(b"gss-step-2"),
            Bytes::from_static(b"gss-step-3"),
            Bytes::from_static(b"gss-final"),
        ]);
        let mut state = AuthChallengeState::new();
        let challenges = [
            (b"server-nonce-1".to_vec(), 21u32),
            (b"server-nonce-2".to_vec(), 21u32),
            (b"server-nonce-3".to_vec(), 21u32),
        ];
        let expected_replies: [&[u8]; 3] = [b"gss-step-2", b"gss-step-3", b"gss-final"];
        for (idx, (challenge, version)) in challenges.iter().enumerate() {
            let cmd = pb::CommandAuthChallenge {
                server_version: Some("test/0".to_owned()),
                challenge: Some(pb::AuthData {
                    auth_method_name: Some("sasl".to_owned()),
                    auth_data: Some(challenge.clone()),
                }),
                protocol_version: Some(*version as i32),
            };
            let response = state
                .handle_challenge(&cmd, &provider)
                .expect("challenge response");
            // After each round, in_progress flips back to idle. completed
            // counts every challenge — pin the cumulative count.
            assert!(!state.is_in_progress());
            assert_eq!(state.completed() as usize, idx + 1);
            let inner = response.response.expect("response payload");
            assert_eq!(inner.auth_method_name.as_deref(), Some("sasl"));
            assert_eq!(inner.auth_data.as_deref(), Some(expected_replies[idx]));
            assert_eq!(response.protocol_version, Some(*version as i32));
            // Provider must have observed the most recent broker challenge.
            assert_eq!(
                provider
                    .last_challenge
                    .lock()
                    .expect("mutex poisoned")
                    .as_deref(),
                Some(challenge.as_slice()),
            );
        }
        assert_eq!(state.completed(), 3);
    }
}
