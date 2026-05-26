// SPDX-License-Identifier: Apache-2.0

//! Tokio runtime layer for SASL Kerberos / GSSAPI: validates that
//! [`magnetar_auth_sasl::SaslKerberos`] plugs into
//! [`magnetar_runtime_tokio::ConnectionShared::with_auth`] and that the
//! protocol-layer `AuthChallengeState` threads each
//! `CommandAuthChallenge` through `respond_to_challenge` so a multi-round
//! GSSAPI initiate loop completes against scripted broker challenges.
//!
//! Uses [`magnetar_auth_sasl::ScriptedGssapiClient`] so the test stays
//! hermetic — no real KDC, no `libgssapi` dependency. The four-layer test
//! set per ADR-0024 pairs this with the moonpool sibling under
//! `crates/magnetar-runtime-moonpool/tests/sasl_kerberos_challenge.rs` and
//! the differential equivalence test under
//! `crates/magnetar-differential/tests/sasl_kerberos_equivalence.rs`.

use std::sync::Arc;

use bytes::Bytes;
use magnetar_auth_sasl::{GssapiClient, SaslKerberos, ScriptedGssapiClient, ScriptedStep};
use magnetar_proto::auth::{AuthChallengeState, AuthProvider};
use magnetar_proto::{ConnectionConfig, pb};

/// Three-step GSSAPI transcript: initial AP-REQ → mutual-auth nonce →
/// SASL layer-choice confirmation. Mirrors the typical Pulsar broker SASL
/// dialogue against a Kerberos-aware broker.
fn three_step_transcript() -> Vec<ScriptedStep> {
    vec![
        ScriptedStep::expecting(b"".as_ref(), b"gss-ap-req".as_ref(), true),
        ScriptedStep::expecting(
            b"server-mutual-nonce".as_ref(),
            b"gss-ap-rep".as_ref(),
            true,
        ),
        ScriptedStep::expecting(b"sasl-layer-choice".as_ref(), b"sasl-final".as_ref(), false),
    ]
}

/// Drive a `SaslKerberos` provider through three `CommandAuthChallenge`
/// rounds via the protocol-layer `AuthChallengeState` helper. Asserts:
/// 1. each round produces the scripted continuation token,
/// 2. `auth_method_name = "sasl"` on every emitted `CommandAuthResponse`,
/// 3. `AuthChallengeState::completed()` increments per round,
/// 4. the underlying `GssapiClient` reports `is_complete` after the final step.
#[test]
fn sasl_kerberos_multi_round_challenge_completes_on_tokio() {
    let scripted = Arc::new(ScriptedGssapiClient::new(three_step_transcript()));
    let provider = SaslKerberos::with_gssapi_client(scripted.clone());

    // Round 0: provider's initial() (empty challenge by contract).
    let initial = provider.initial().expect("initial");
    assert_eq!(initial.as_ref(), b"gss-ap-req");

    let mut state = AuthChallengeState::new();
    let dyn_provider: &dyn AuthProvider = &provider;

    // Round 1: broker → CommandAuthChallenge("server-mutual-nonce").
    let cmd_1 = pb::CommandAuthChallenge {
        server_version: Some("test/0".to_owned()),
        challenge: Some(pb::AuthData {
            auth_method_name: Some("sasl".to_owned()),
            auth_data: Some(b"server-mutual-nonce".to_vec()),
        }),
        protocol_version: Some(21),
    };
    let resp_1 = state
        .handle_challenge(&cmd_1, dyn_provider)
        .expect("round 1");
    let inner = resp_1.response.expect("round 1 payload");
    assert_eq!(inner.auth_method_name.as_deref(), Some("sasl"));
    assert_eq!(inner.auth_data.as_deref(), Some(b"gss-ap-rep".as_slice()));
    assert_eq!(resp_1.protocol_version, Some(21));
    assert_eq!(state.completed(), 1);

    // Round 2: broker → CommandAuthChallenge("sasl-layer-choice"), final.
    let cmd_2 = pb::CommandAuthChallenge {
        server_version: Some("test/0".to_owned()),
        challenge: Some(pb::AuthData {
            auth_method_name: Some("sasl".to_owned()),
            auth_data: Some(b"sasl-layer-choice".to_vec()),
        }),
        protocol_version: Some(21),
    };
    let resp_2 = state
        .handle_challenge(&cmd_2, dyn_provider)
        .expect("round 2");
    let inner = resp_2.response.expect("round 2 payload");
    assert_eq!(inner.auth_data.as_deref(), Some(b"sasl-final".as_slice()));
    assert_eq!(state.completed(), 2);

    assert!(
        scripted.is_complete(),
        "GSSAPI client must report complete after the final step",
    );
    assert_eq!(scripted.step_count(), 3);
}

/// Smoke check: the provider plugs into `ConnectionShared::with_auth`
/// exactly the way the tokio driver loop expects (PIP-30 / PIP-292
/// in-band auth refresh path). Construction succeeds — the auth provider
/// is now reachable from `ConnectionShared::auth_provider`, which is
/// what `handle_pending_events` consults on every `AuthChallenge`.
#[test]
fn sasl_kerberos_plugs_into_connection_shared() {
    // Single-step transcript accepting any challenge so the test focuses
    // purely on the plug-through: provider goes into `with_auth`, comes
    // out the other side, and `respond_to_challenge` reaches the wrapped
    // `GssapiClient`.
    let scripted = Arc::new(ScriptedGssapiClient::new(vec![ScriptedStep::anything(
        b"gss-final".as_ref(),
        false,
    )]));
    let provider: Arc<dyn AuthProvider> = Arc::new(SaslKerberos::with_gssapi_client(scripted));
    let shared = magnetar_runtime_tokio::ConnectionShared::with_auth(
        ConnectionConfig::default(),
        Some(provider),
    );
    // The driver consults `shared.auth_provider`; verify the slot is
    // populated and the challenge byte path reaches the GssapiClient.
    let stored = shared.auth_provider.as_ref().expect("auth provider stored");
    let bytes = stored
        .respond_to_challenge(b"server-mutual-nonce")
        .expect("challenge");
    assert_eq!(bytes, Bytes::from_static(b"gss-final"));
}
