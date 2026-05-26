// SPDX-License-Identifier: Apache-2.0

//! SASL auth providers for magnetar.
//!
//! Two mechanisms are exposed:
//!
//! - [`SaslPlain`] — RFC 4616 `PLAIN` mechanism. Useful for username/password broker auth in tests
//!   and for environments where token-based auth is not configured.
//! - [`SaslKerberos`] — GSS-API / Kerberos mechanism. Backed by the `libgssapi` system binding
//!   under the `kerberos` cargo feature; otherwise wired against a user-supplied [`GssapiClient`]
//!   (typically the in-tree [`ScriptedGssapiClient`] for tests).
//!
//! In both cases the Pulsar `auth_method_name` reported through
//! [`AuthProvider::method`](magnetar_proto::AuthProvider::method) is
//! `"sasl"`, matching `org.apache.pulsar.client.impl.auth.AuthenticationSasl`.
//!
//! # Multi-step handshake
//!
//! Kerberos completes over multiple `CommandAuthChallenge` /
//! `CommandAuthResponse` round-trips. The protocol layer threads each
//! challenge through
//! [`AuthProvider::respond_to_challenge`](magnetar_proto::AuthProvider::respond_to_challenge)
//! on every event; [`SaslKerberos`] simply forwards into the wrapped
//! [`GssapiClient`] so the GSSAPI step loop runs naturally over the
//! `magnetar-proto` `AuthChallengeState` driver. See
//! [ADR-0029](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0029-sasl-kerberos-gssapi-scope.md)
//! for the design.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

mod kerberos;
mod plain;

#[cfg(feature = "kerberos")]
mod gssapi;

#[cfg(feature = "kerberos")]
pub use gssapi::LibGssapiClient;
pub use kerberos::{
    GssapiClient, GssapiError, GssapiStep, SaslKerberos, ScriptedGssapiClient, ScriptedStep,
};
pub use plain::SaslPlain;
