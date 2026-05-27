// SPDX-License-Identifier: Apache-2.0

//! Differential equivalence (ADR-0024 layer 4) for SASL Kerberos / GSSAPI:
//! the same `SaslKerberos` provider, driven through both runtime layers'
//! `AuthChallengeState` wiring, must produce byte-identical
//! `CommandAuthResponse` payloads.
//!
//! The provider itself is engine-agnostic — it carries the GSSAPI step
//! loop entirely inside `magnetar-proto::auth::AuthProvider`. This test
//! pins that contract: if a future refactor accidentally specialises the
//! engine layer (e.g. caches the response on the tokio side but not the
//! moonpool side), the differential test trips.

use std::sync::Arc;

use magnetar_auth_sasl::{SaslKerberos, ScriptedGssapiClient, ScriptedStep};
use magnetar_proto::auth::{AuthChallengeState, AuthProvider};
use magnetar_proto::{ConnectionConfig, pb};

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

/// Drive a fresh `SaslKerberos` through the multi-round handshake under a
/// freshly-constructed `ConnectionShared` from the requested engine, and
/// collect the emitted `CommandAuthResponse` bytes per round.
///
/// Returns `(initial_bytes, [response_bytes; 2])`. The engine flavour is
/// chosen via the `engine` argument so a single test body can compare
/// tokio against moonpool.
fn run_handshake_under(engine: Engine) -> (Vec<u8>, [Vec<u8>; 2]) {
    let scripted = Arc::new(ScriptedGssapiClient::new(three_step_transcript()));
    let provider: Arc<dyn AuthProvider> = Arc::new(SaslKerberos::with_gssapi_client(scripted));

    // Anchor the provider against an engine's `ConnectionShared::with_auth`
    // to make sure the engine boundary doesn't accidentally project a
    // different `AuthProvider` view (e.g. via a wrapper or a clone).
    let stored = match engine {
        Engine::Tokio => {
            let shared = magnetar_runtime_tokio::ConnectionShared::with_auth(
                ConnectionConfig::default(),
                Some(provider),
            );
            shared
                .auth_provider
                .as_ref()
                .expect("tokio auth provider")
                .clone()
        }
        Engine::Moonpool => {
            let shared = magnetar_runtime_moonpool::ConnectionShared::with_auth(
                ConnectionConfig::default(),
                Some(provider),
            );
            shared
                .auth_provider
                .as_ref()
                .expect("moonpool auth provider")
                .clone()
        }
    };

    let initial = stored.initial().expect("initial");

    let mut state = AuthChallengeState::new();
    let mut rounds = [Vec::new(), Vec::new()];
    for (idx, server_token) in [
        b"server-mutual-nonce".as_ref(),
        b"sasl-layer-choice".as_ref(),
    ]
    .iter()
    .enumerate()
    {
        let cmd = pb::CommandAuthChallenge {
            server_version: Some("test/0".to_owned()),
            challenge: Some(pb::AuthData {
                auth_method_name: Some("sasl".to_owned()),
                auth_data: Some(bytes::Bytes::copy_from_slice(server_token)),
            }),
            protocol_version: Some(21),
        };
        let response = state
            .handle_challenge(&cmd, &*stored)
            .expect("handle_challenge");
        rounds[idx] = response
            .response
            .expect("payload")
            .auth_data
            .unwrap()
            .to_vec();
    }

    (initial.to_vec(), rounds)
}

#[derive(Debug, Clone, Copy)]
enum Engine {
    Tokio,
    Moonpool,
}

/// Both engines must emit byte-identical SASL Kerberos transcripts for
/// the same scripted GSSAPI client. If they ever diverge, the SASL state
/// machine has acquired engine-specific behaviour — a bug per ADR-0019.
#[test]
fn sasl_kerberos_handshake_bytes_identical_across_engines() {
    let (initial_tokio, rounds_tokio) = run_handshake_under(Engine::Tokio);
    let (initial_moonpool, rounds_moonpool) = run_handshake_under(Engine::Moonpool);

    assert_eq!(
        initial_tokio, initial_moonpool,
        "initial GSSAPI token must match across engines (tokio={initial_tokio:?}, \
         moonpool={initial_moonpool:?})",
    );
    assert_eq!(
        rounds_tokio, rounds_moonpool,
        "per-round AUTH_RESPONSE bytes must match across engines",
    );
    // Sanity: the test would still pass on an empty transcript above; pin
    // the actual content so an accidental empty-transcript drift would
    // also trip.
    assert_eq!(initial_tokio.as_slice(), b"gss-ap-req");
    assert_eq!(rounds_tokio[0].as_slice(), b"gss-ap-rep");
    assert_eq!(rounds_tokio[1].as_slice(), b"sasl-final");
}
