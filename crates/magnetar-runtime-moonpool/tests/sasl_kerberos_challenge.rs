// SPDX-License-Identifier: Apache-2.0

//! Moonpool runtime layer for SASL Kerberos / GSSAPI. Mirrors
//! `crates/magnetar-runtime-tokio/tests/sasl_kerberos_challenge.rs`
//! one-to-one per ADR-0024's runtime test parity contract.
//!
//! The moonpool engine has no real KDC and no `libgssapi` linkage; the
//! sans-io state machine is driven over scripted GSSAPI tokens via
//! [`magnetar_auth_sasl::ScriptedGssapiClient`], identical to the tokio
//! sibling. This gives us deterministic bit-for-bit reproducibility of
//! the GSSAPI initiate loop without standing up a KDC.

use std::sync::Arc;

use bytes::Bytes;
use magnetar_auth_sasl::{GssapiClient, SaslKerberos, ScriptedGssapiClient, ScriptedStep};
use magnetar_proto::auth::{AuthChallengeState, AuthProvider};
use magnetar_proto::{ConnectionConfig, pb};

/// Three-step GSSAPI transcript: initial AP-REQ → mutual-auth nonce →
/// SASL layer-choice confirmation. Identical to the tokio sibling so the
/// differential equivalence test under
/// `crates/magnetar-differential/tests/sasl_kerberos_equivalence.rs` can
/// pin byte-identical responses across both engines.
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

/// Three-round GSSAPI exchange completes against scripted broker
/// challenges via the protocol-layer `AuthChallengeState`. Asserts the
/// same invariants as the tokio sibling.
#[test]
fn sasl_kerberos_multi_round_challenge_completes_on_moonpool() {
    let scripted = Arc::new(ScriptedGssapiClient::new(three_step_transcript()));
    let provider = SaslKerberos::with_gssapi_client(scripted.clone());

    let initial = provider.initial().expect("initial");
    assert_eq!(initial.as_ref(), b"gss-ap-req");

    let mut state = AuthChallengeState::new();
    let dyn_provider: &dyn AuthProvider = &provider;

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

/// Moonpool sibling of the tokio smoke test: provider plugs into
/// `ConnectionShared::with_auth` so the driver's `handle_pending_events`
/// route can consult it. Maintains the 1:1 test count with the tokio
/// runtime tests file (ADR-0024 §runtime-parity).
#[test]
fn sasl_kerberos_plugs_into_moonpool_connection_shared() {
    // Single-step transcript accepting any challenge so the test focuses
    // purely on the plug-through: provider goes into `with_auth`, comes
    // out the other side, and `respond_to_challenge` reaches the wrapped
    // `GssapiClient`.
    let scripted = Arc::new(ScriptedGssapiClient::new(vec![ScriptedStep::anything(
        b"gss-final".as_ref(),
        false,
    )]));
    let provider: Arc<dyn AuthProvider> = Arc::new(SaslKerberos::with_gssapi_client(scripted));
    let shared = magnetar_runtime_moonpool::ConnectionShared::with_auth(
        ConnectionConfig::default(),
        Some(provider),
    );
    let stored = shared.auth_provider.as_ref().expect("auth provider stored");
    let bytes = stored
        .respond_to_challenge(b"server-mutual-nonce")
        .expect("challenge");
    assert_eq!(bytes, Bytes::from_static(b"gss-final"));
}
