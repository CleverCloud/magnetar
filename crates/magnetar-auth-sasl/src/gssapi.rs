// SPDX-License-Identifier: Apache-2.0

//! `libgssapi` adapter — production [`GssapiClient`] backed by a system
//! GSS-API library (MIT KRB5 or Heimdal).
//!
//! Only compiled with the `kerberos` cargo feature so the sans-io test
//! layers stay free of a `libgssapi` build dependency (per ADR-0029 +
//! ADR-0024).
//!
//! # API mapping
//!
//! `libgssapi::context::ClientCtx::step` is shaped exactly like the
//! GSS-API `gss_init_sec_context` call: pass the latest server token (or
//! `None` for the first call), get back the next client token (or `None`
//! when the exchange is complete). The mapping onto our
//! [`GssapiClient::step`] is one-to-one:
//!
//! - empty `challenge` ↔ `step(None)` (initial token)
//! - non-empty `challenge` ↔ `step(Some(challenge))` (continuation)
//! - `Some(tok)` from `libgssapi` ↔ `GssapiStep { token, continue_needed }`
//! - `None` from `libgssapi` ↔ `GssapiStep { token: empty, continue_needed: false }`
//!
//! The Kerberos GSS mechanism OID and `NT_KRB5_PRINCIPAL` name type
//! correspond to the broker service principal form Pulsar uses
//! (`<server-type>/<broker-host>@<REALM>`).

use bytes::Bytes;
use libgssapi::context::{ClientCtx, CtxFlags, SecurityContext};
use libgssapi::credential::{Cred, CredUsage};
use libgssapi::name::Name;
use libgssapi::oid::{GSS_MECH_KRB5, GSS_NT_KRB5_PRINCIPAL, OidSet};
use parking_lot::Mutex;

use crate::kerberos::{GssapiClient, GssapiError, GssapiStep};

/// Production [`GssapiClient`] backed by `libgssapi::context::ClientCtx`.
///
/// Construct via [`Self::new`] with a Kerberos service-principal string of
/// the form `<server-type>/<broker-host>@<REALM>` (e.g.
/// `pulsar/broker.example@EXAMPLE.COM`). The default initiator credentials
/// from the local credential cache (or keytab via the system Kerberos
/// config) are used; magnetar v0.2.0 does not (yet) take an explicit
/// keytab path — that is delegated to the surrounding krb5.conf /
/// `KRB5_CLIENT_KTNAME` environment setup, matching how the Java client's
/// `useTicketCache=true` default behaves.
///
/// `ClientCtx::step` takes `&mut self` because each call mutates the
/// GSSAPI state machine, but the [`GssapiClient`] surface is `&self`
/// (the provider is shared across the driver task and user-facing
/// futures via `Arc<dyn AuthProvider>`). The `ClientCtx` is therefore
/// wrapped in a `parking_lot::Mutex` here. Contention is negligible —
/// the auth path is sequential per connection — but the wrapping is
/// load-bearing for correctness, not just convenience.
#[derive(Debug)]
pub struct LibGssapiClient {
    ctx: Mutex<ClientCtx>,
}

impl LibGssapiClient {
    /// Build a client targeting `service_principal`.
    ///
    /// # Errors
    ///
    /// Surfaces [`GssapiError::Library`] when `libgssapi` rejects either
    /// the principal parse, the credential acquisition, or the context
    /// construction (missing default credential cache, KDC unreachable,
    /// unsupported mechanism).
    pub fn new(service_principal: &str) -> Result<Self, GssapiError> {
        let target = Name::new(service_principal.as_bytes(), Some(&GSS_NT_KRB5_PRINCIPAL))
            .map_err(|err| GssapiError::Library(format!("Name::new failed: {err}")))?;
        let target = target
            .canonicalize(Some(&GSS_MECH_KRB5))
            .map_err(|err| GssapiError::Library(format!("Name::canonicalize failed: {err}")))?;

        let mut desired_mechs = OidSet::new()
            .map_err(|err| GssapiError::Library(format!("OidSet::new failed: {err}")))?;
        desired_mechs
            .add(&GSS_MECH_KRB5)
            .map_err(|err| GssapiError::Library(format!("OidSet::add failed: {err}")))?;

        let cred = Cred::acquire(None, None, CredUsage::Initiate, Some(&desired_mechs))
            .map_err(|err| GssapiError::Library(format!("Cred::acquire failed: {err}")))?;
        let ctx = ClientCtx::new(
            Some(cred),
            target,
            CtxFlags::GSS_C_MUTUAL_FLAG,
            Some(&GSS_MECH_KRB5),
        );
        Ok(Self {
            ctx: Mutex::new(ctx),
        })
    }
}

impl GssapiClient for LibGssapiClient {
    fn step(&self, challenge: &[u8]) -> Result<GssapiStep, GssapiError> {
        let server_tok = if challenge.is_empty() {
            None
        } else {
            Some(challenge)
        };
        let mut ctx = self.ctx.lock();
        let result = ctx
            .step(server_tok, None)
            .map_err(|err| GssapiError::Library(format!("ClientCtx::step failed: {err}")))?;
        let token = match result {
            Some(buf) => Bytes::copy_from_slice(&buf),
            None => Bytes::new(),
        };
        let continue_needed = !ctx.is_complete();
        Ok(GssapiStep {
            token,
            continue_needed,
        })
    }

    fn is_complete(&self) -> bool {
        self.ctx.lock().is_complete()
    }
}
