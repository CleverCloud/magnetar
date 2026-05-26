// SPDX-License-Identifier: Apache-2.0

//! SASL Kerberos / GSSAPI provider.
//!
//! Mirrors `org.apache.pulsar.client.impl.auth.AuthenticationSasl` in its GSSAPI
//! configuration: a multi-step CONNECT → `AUTH_CHALLENGE` → `AUTH_RESPONSE`
//! exchange that runs the GSS-API initiate-loop client-side and feeds the
//! resulting tokens through the broker until the security context is
//! established.
//!
//! # Surface
//!
//! - [`SaslKerberos`] is an [`AuthProvider`] whose `initial` returns the GSS-API first-call token
//!   and whose `respond_to_challenge` feeds each broker challenge back into the GSS-API context.
//! - [`GssapiClient`] is the sans-io seam between the provider and any GSSAPI binding.
//!   Implementations track context state internally (mutex over `libgssapi`'s `ClientCtx`, or a
//!   scripted token queue in tests).
//! - [`LibGssapiClient`] (only with the `kerberos` cargo feature) is the production binding built
//!   on top of `libgssapi::context::ClientCtx`.
//! - [`ScriptedGssapiClient`] is always available and replays a precomputed token transcript — used
//!   by the four sans-io test layers (per ADR-0024) so the wire behaviour is verified without a
//!   live KDC.
//!
//! The split puts the only `unsafe` / FFI code behind the `kerberos` feature;
//! the protocol-correctness tests run on every CI matrix cell without needing
//! `libkrb5` on the build host.
//!
//! # Mechanism semantics
//!
//! The Pulsar broker reports `auth_method_name = "sasl"`. The first
//! `auth_data` payload carries the GSSAPI initial token; the broker replies
//! with `CommandAuthChallenge` until the SASL state machine completes. We
//! return [`AuthProvider::method`] = `"sasl"` to match this.
//!
//! On completion the GSSAPI client's `is_complete` flag flips. Some broker
//! configurations send one final empty challenge to confirm the handshake;
//! [`SaslKerberos`] returns the empty token in that case rather than erroring,
//! mirroring the Java client.

use core::fmt;
use std::sync::Arc;

use bytes::Bytes;
use magnetar_proto::{AuthError, AuthProvider};
use parking_lot::Mutex;

/// Sans-io GSSAPI seam consumed by [`SaslKerberos`].
///
/// Every call to [`Self::step`] is one round of the GSS-API initiate loop.
/// `challenge` is empty on the very first call (the provider's
/// [`AuthProvider::initial`] entry); subsequent calls carry the broker's
/// `CommandAuthChallenge.auth_data` bytes.
///
/// Implementations track context state themselves — `SaslKerberos` never
/// caches anything beyond a thin in-progress marker.
pub trait GssapiClient: Send + Sync + fmt::Debug {
    /// Drive one step of the GSSAPI exchange.
    ///
    /// # Errors
    ///
    /// Returns [`GssapiError`] if the underlying binding rejects the
    /// challenge (malformed token, mechanism mismatch, expired credentials).
    fn step(&self, challenge: &[u8]) -> Result<GssapiStep, GssapiError>;

    /// `true` once the security context has been fully established.
    ///
    /// The provider consults this after each step to short-circuit final
    /// empty challenges from the broker.
    fn is_complete(&self) -> bool;
}

/// Result of one GSSAPI step.
#[derive(Debug, Clone)]
pub struct GssapiStep {
    /// Token bytes to ship to the broker. May be empty on the final step.
    pub token: Bytes,
    /// `true` if the GSSAPI implementation expects to receive at least one
    /// more challenge. `false` means the security context is established
    /// on the client side; the broker may still send one final confirmation
    /// challenge depending on its policy.
    pub continue_needed: bool,
}

/// Errors surfaced by a [`GssapiClient`].
#[derive(Debug, thiserror::Error)]
pub enum GssapiError {
    /// The broker sent a challenge after the security context was reported
    /// complete by the client. This indicates a broker-side protocol bug.
    #[error("GSSAPI exchange already complete; broker sent an unexpected challenge")]
    AlreadyComplete,

    /// The challenge bytes were malformed or did not match the negotiated
    /// mechanism.
    #[error("invalid GSSAPI challenge: {0}")]
    Invalid(String),

    /// The underlying GSSAPI library refused the call (credentials missing,
    /// keytab unreadable, KDC unreachable, mutual-auth failure, …).
    #[error("GSSAPI library error: {0}")]
    Library(String),
}

impl From<GssapiError> for AuthError {
    fn from(err: GssapiError) -> Self {
        match err {
            GssapiError::AlreadyComplete | GssapiError::Invalid(_) => {
                AuthError::Invalid(err.to_string())
            }
            GssapiError::Library(_) => AuthError::Provider(Box::new(err)),
        }
    }
}

/// SASL Kerberos / GSSAPI auth provider.
///
/// Wraps a [`GssapiClient`] that owns the GSS-API security context. The
/// provider itself is stateless beyond holding the client `Arc` — every
/// `initial` / `respond_to_challenge` call delegates straight into the
/// client.
///
/// Construct via [`Self::with_gssapi_client`] when wiring a custom
/// [`GssapiClient`] (typically a test fake), or via `with_principal`
/// (only compiled under the `kerberos` cargo feature) to bind to a real
/// `libgssapi`-backed context.
#[derive(Debug, Clone)]
pub struct SaslKerberos {
    client: Arc<dyn GssapiClient>,
}

impl SaslKerberos {
    /// Build a provider around an externally-managed [`GssapiClient`].
    ///
    /// Available without the `kerberos` feature so the sans-io test layers
    /// (per ADR-0024) can drive [`ScriptedGssapiClient`] without pulling in
    /// `libgssapi` on the build host.
    #[must_use]
    pub fn with_gssapi_client(client: Arc<dyn GssapiClient>) -> Self {
        Self { client }
    }

    /// Build a provider that binds to the local Kerberos credential cache and
    /// targets the given service principal (typically
    /// `pulsar/<broker-host>@<REALM>`).
    ///
    /// Only available with the `kerberos` cargo feature. On systems without
    /// `libkrb5` / `libgssapi`, prefer [`Self::with_gssapi_client`] paired
    /// with a custom [`GssapiClient`].
    ///
    /// # Errors
    ///
    /// Returns [`GssapiError::Library`] when `libgssapi` rejects the call
    /// (missing default credential cache, malformed service name, KDC
    /// unreachable).
    #[cfg(feature = "kerberos")]
    pub fn with_principal(service_principal: &str) -> Result<Self, GssapiError> {
        let client = crate::gssapi::LibGssapiClient::new(service_principal)?;
        Ok(Self::with_gssapi_client(Arc::new(client)))
    }

    /// Reference to the configured GSSAPI client. Mostly useful in tests that
    /// want to assert on a fake's call count.
    #[must_use]
    pub fn gssapi_client(&self) -> &Arc<dyn GssapiClient> {
        &self.client
    }
}

impl AuthProvider for SaslKerberos {
    fn method(&self) -> &str {
        "sasl"
    }

    fn initial(&self) -> Result<Bytes, AuthError> {
        let step = self.client.step(&[])?;
        Ok(step.token)
    }

    fn respond_to_challenge(&self, challenge: &[u8]) -> Result<Bytes, AuthError> {
        // Some broker configurations confirm the handshake with one final
        // zero-length challenge after the client-side context flips to
        // complete. Mirror the Java client by returning empty bytes rather
        // than erroring out — the broker accepts an empty `auth_data`.
        if self.client.is_complete() && challenge.is_empty() {
            return Ok(Bytes::new());
        }
        let step = self.client.step(challenge)?;
        Ok(step.token)
    }
}

/// Test [`GssapiClient`] that replays a precomputed transcript of GSSAPI
/// steps.
///
/// The scripted client lets every test layer per ADR-0024 (proto, tokio,
/// moonpool, differential, e2e) drive the SASL state machine without a real
/// KDC. Each call to [`GssapiClient::step`] pops the next entry off the
/// transcript and returns it; once the transcript is drained the client
/// reports `is_complete = true` and surfaces [`GssapiError::AlreadyComplete`]
/// for any further calls.
///
/// Construct via [`Self::new`] with a list of `(input_challenge,
/// output_token, continue_needed)` triples. The `input_challenge` field is
/// asserted against the bytes the caller passes — that's how tests pin the
/// continuation thread.
pub struct ScriptedGssapiClient {
    transcript: Mutex<std::vec::IntoIter<ScriptedStep>>,
    complete: Mutex<bool>,
    steps: Mutex<u32>,
}

/// One scripted GSSAPI exchange row.
#[derive(Debug, Clone)]
pub struct ScriptedStep {
    /// Expected bytes the broker (or the provider's `initial()` call) will
    /// hand to the client. `None` matches anything; `Some(bytes)` is asserted
    /// for equality.
    pub expect_challenge: Option<Bytes>,
    /// Bytes the client should return for this step.
    pub reply: Bytes,
    /// Whether the SASL state machine expects another challenge after this
    /// step. The final step typically sets this to `false`.
    pub continue_needed: bool,
}

impl ScriptedStep {
    /// Convenience: build a step that matches any challenge and replies with
    /// `reply`. `continue_needed = true` means the SASL handshake is not yet
    /// done.
    #[must_use]
    pub fn anything(reply: impl Into<Bytes>, continue_needed: bool) -> Self {
        Self {
            expect_challenge: None,
            reply: reply.into(),
            continue_needed,
        }
    }

    /// Convenience: build a step that asserts the broker sent
    /// `expected_challenge` and replies with `reply`.
    #[must_use]
    pub fn expecting(
        expected_challenge: impl Into<Bytes>,
        reply: impl Into<Bytes>,
        continue_needed: bool,
    ) -> Self {
        Self {
            expect_challenge: Some(expected_challenge.into()),
            reply: reply.into(),
            continue_needed,
        }
    }
}

impl ScriptedGssapiClient {
    /// Construct a scripted client with a fixed transcript.
    #[must_use]
    pub fn new(transcript: Vec<ScriptedStep>) -> Self {
        Self {
            transcript: Mutex::new(transcript.into_iter()),
            complete: Mutex::new(false),
            steps: Mutex::new(0),
        }
    }

    /// Number of `step()` calls observed so far. Used by tests to assert the
    /// expected number of round-trips.
    #[must_use]
    pub fn step_count(&self) -> u32 {
        *self.steps.lock()
    }
}

impl fmt::Debug for ScriptedGssapiClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `transcript` is a `std::vec::IntoIter` — it has no Debug we can
        // surface without consuming the iterator, so we render only the
        // observable counters. The "transcript" field is opaque on
        // purpose; clippy::missing_fields_in_debug is silenced because
        // the omission is intentional, not an oversight.
        let _ = &self.transcript;
        f.debug_struct("ScriptedGssapiClient")
            .field("steps", &self.step_count())
            .field("complete", &*self.complete.lock())
            .field("transcript", &"<opaque>")
            .finish()
    }
}

impl GssapiClient for ScriptedGssapiClient {
    fn step(&self, challenge: &[u8]) -> Result<GssapiStep, GssapiError> {
        if *self.complete.lock() {
            return Err(GssapiError::AlreadyComplete);
        }
        let next = self.transcript.lock().next().ok_or_else(|| {
            GssapiError::Invalid(
                "scripted transcript exhausted before broker reported success".to_owned(),
            )
        })?;
        if let Some(expected) = next.expect_challenge.as_ref() {
            if expected.as_ref() != challenge {
                return Err(GssapiError::Invalid(format!(
                    "scripted step {} expected challenge {:?}, got {:?}",
                    *self.steps.lock(),
                    expected.as_ref(),
                    challenge,
                )));
            }
        }
        *self.steps.lock() += 1;
        if !next.continue_needed {
            *self.complete.lock() = true;
        }
        Ok(GssapiStep {
            token: next.reply,
            continue_needed: next.continue_needed,
        })
    }

    fn is_complete(&self) -> bool {
        *self.complete.lock()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use magnetar_proto::AuthProvider;

    use super::{GssapiClient, GssapiError, SaslKerberos, ScriptedGssapiClient, ScriptedStep};

    fn three_step_transcript() -> Vec<ScriptedStep> {
        vec![
            // Initial: provider feeds `&[]`, GSSAPI returns AP-REQ-like blob.
            ScriptedStep::expecting(b"".as_ref(), b"ap-req-token".as_ref(), true),
            // Broker challenges with mutual-auth nonce; client wraps.
            ScriptedStep::expecting(
                b"server-mutual-nonce".as_ref(),
                b"ap-rep-token".as_ref(),
                true,
            ),
            // Final confirmation: client returns the SASL conf/integ layer
            // selection blob; the SASL state machine reports complete.
            ScriptedStep::expecting(b"sasl-layer-choice".as_ref(), b"sasl-final".as_ref(), false),
        ]
    }

    #[test]
    fn method_is_sasl() {
        let provider = SaslKerberos::with_gssapi_client(Arc::new(ScriptedGssapiClient::new(vec![
            ScriptedStep::anything(b"x".as_ref(), false),
        ])));
        assert_eq!(provider.method(), "sasl");
    }

    #[test]
    fn initial_feeds_empty_challenge() {
        let client = Arc::new(ScriptedGssapiClient::new(three_step_transcript()));
        let provider = SaslKerberos::with_gssapi_client(client.clone());
        let bytes = provider.initial().expect("initial");
        assert_eq!(bytes.as_ref(), b"ap-req-token");
        assert_eq!(client.step_count(), 1);
        assert!(!client.is_complete());
    }

    #[test]
    fn respond_to_challenge_threads_continuation_tokens() {
        let client = Arc::new(ScriptedGssapiClient::new(three_step_transcript()));
        let provider = SaslKerberos::with_gssapi_client(client.clone());

        let _ = provider.initial().expect("initial");
        let r1 = provider
            .respond_to_challenge(b"server-mutual-nonce")
            .expect("step 2");
        assert_eq!(r1.as_ref(), b"ap-rep-token");
        assert!(!client.is_complete());

        let r2 = provider
            .respond_to_challenge(b"sasl-layer-choice")
            .expect("step 3");
        assert_eq!(r2.as_ref(), b"sasl-final");
        assert!(client.is_complete());
        assert_eq!(client.step_count(), 3);
    }

    #[test]
    fn empty_challenge_after_complete_returns_empty_bytes() {
        // Some brokers send one zero-length confirmation challenge after the
        // client-side context flips to complete. Returning empty bytes (no
        // error) matches the Java client.
        let client = Arc::new(ScriptedGssapiClient::new(vec![ScriptedStep::anything(
            b"final".as_ref(),
            false,
        )]));
        let provider = SaslKerberos::with_gssapi_client(client.clone());
        let _ = provider.initial().expect("initial");
        assert!(client.is_complete());
        let extra = provider
            .respond_to_challenge(&[])
            .expect("empty post-complete must not error");
        assert!(extra.is_empty());
    }

    #[test]
    fn nonempty_challenge_after_complete_surfaces_invalid() {
        let client = Arc::new(ScriptedGssapiClient::new(vec![ScriptedStep::anything(
            b"final".as_ref(),
            false,
        )]));
        let provider = SaslKerberos::with_gssapi_client(client.clone());
        let _ = provider.initial().expect("initial");
        let err = provider
            .respond_to_challenge(b"unexpected")
            .expect_err("non-empty challenge after complete must surface error");
        assert!(format!("{err}").contains("already complete"), "err={err}");
    }

    #[test]
    fn challenge_mismatch_surfaces_invalid() {
        let client = Arc::new(ScriptedGssapiClient::new(three_step_transcript()));
        let provider = SaslKerberos::with_gssapi_client(client);
        let _ = provider.initial().expect("initial");
        let err = provider
            .respond_to_challenge(b"WRONG")
            .expect_err("mismatched scripted challenge must error");
        assert!(format!("{err}").contains("expected"), "err={err}");
    }

    #[test]
    fn gssapi_error_maps_to_provider_for_library_failures() {
        let err: magnetar_proto::AuthError =
            GssapiError::Library("krb5_cc_default: No such file".to_owned()).into();
        match err {
            magnetar_proto::AuthError::Provider(_) => {}
            other => panic!("expected Provider variant, got {other:?}"),
        }
    }
}
