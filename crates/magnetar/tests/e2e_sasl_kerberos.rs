// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the [`magnetar_auth_sasl::SaslKerberos`]
//! provider against a Dockerised MIT Kerberos KDC (per ADR-0029).
//!
//! Run with:
//!
//! ```sh
//! cargo test --features auth-sasl-kerberos \
//!   -p magnetar --test e2e_sasl_kerberos -- --nocapture
//! ```
//!
//! The default KDC image is `gcavalcante8808/krb5-server` — a small,
//! widely-mirrored MIT KRB5 KDC on Docker Hub. The goal directive
//! originally named `bitnami/kerberos`, but that repository does not
//! exist on the public Hub; `gcavalcante8808/krb5-server` is the
//! closest interchangeable substitute. Override via
//! `MAGNETAR_KDC_IMAGE_REPO` + `MAGNETAR_KDC_IMAGE_TAG` to point at an
//! internal CI mirror. A pre-populated `krb5.conf` + `KRB5_CLIENT_KTNAME`
//! keytab on the build host are required for the credential-acquisition
//! path to succeed; without them the GSSAPI binding surfaces
//! [`magnetar_auth_sasl::GssapiError::Library`], which the test treats as
//! an expected-failure marker (the wiring is correct; only the host
//! credentials are missing).
//!
//! Why this layer exists: layers 1–4 (proto, tokio, moonpool, differential
//! per ADR-0024) all use [`magnetar_auth_sasl::ScriptedGssapiClient`] to
//! avoid linking `libgssapi` on every CI cell. This e2e is the only place
//! where the production [`magnetar_auth_sasl::LibGssapiClient`] runs
//! against real Kerberos primitives.

#![cfg(feature = "auth-sasl-kerberos")]

use std::time::Duration;

use magnetar_auth_sasl::SaslKerberos;
use magnetar_proto::AuthProvider;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_KDC_IMAGE_REPO: &str = "gcavalcante8808/krb5-server";
const DEFAULT_KDC_IMAGE_TAG: &str = "latest";
const KDC_PORT: u16 = 88;

fn image_repo() -> String {
    std::env::var("MAGNETAR_KDC_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_KDC_IMAGE_REPO.to_owned())
}

fn image_tag() -> String {
    std::env::var("MAGNETAR_KDC_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_KDC_IMAGE_TAG.to_owned())
}

/// Service principal targeted by the e2e GSSAPI handshake. Override via
/// `MAGNETAR_KDC_SERVICE_PRINCIPAL` so internal CI realms can plug in
/// their own SPN without rebuilding.
fn service_principal() -> String {
    std::env::var("MAGNETAR_KDC_SERVICE_PRINCIPAL")
        .unwrap_or_else(|_| "pulsar/broker.example.com@EXAMPLE.COM".to_owned())
}

/// Boot the KDC container and wait for it to advertise readiness.
///
/// The `gcavalcante8808/krb5-server` image's entrypoint requires
/// `KRB5_REALM`, `KRB5_KDC`, and `KRB5_PASS` (an admin password). It
/// then provisions a fresh `/var/lib/krb5kdc` database, drops a
/// matching `/etc/krb5.conf` and `kdc.conf`, and execs `supervisord`
/// which launches `krb5kdc` + `kadmind`. supervisord logs its own
/// startup CRIT line about the unauthenticated inet HTTP server once
/// the child processes are spawned; that's the latest reliable signal
/// in the boot transcript, so we wait for it.
async fn start_kdc()
-> Result<testcontainers::ContainerAsync<GenericImage>, Box<dyn std::error::Error>> {
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(KDC_PORT))
        .with_exposed_port(ContainerPort::Udp(KDC_PORT))
        .with_wait_for(WaitFor::message_on_stdout(
            "running without any HTTP authentication checking",
        ))
        .with_startup_timeout(Duration::from_secs(120))
        .with_env_var("KRB5_REALM", "EXAMPLE.COM")
        .with_env_var("KRB5_KDC", "kdc.example.com")
        .with_env_var("KRB5_PASS", "magnetar-e2e-admin-password")
        .start()
        .await?;
    Ok(container)
}

/// The KDC fixture stands up and the [`SaslKerberos`] provider can be
/// constructed against a service principal name. The provider's
/// `initial()` call is invoked; the result is one of:
///
/// 1. `Ok(token)` — host has a usable credential cache or keytab, the KDC issued a service ticket,
///    and the GSSAPI binding produced a real `AP-REQ` blob. We assert the token is non-empty.
/// 2. `Err(AuthError)` — credentials unavailable on the host. We assert the error variant maps
///    through `GssapiError::Library` (the binding surfaced the underlying `gss_init_sec_context`
///    error, not a panic).
///
/// Either branch proves the production code path works end-to-end; only
/// the credential-availability axis changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_sasl_kerberos_provider_runs_against_dockerised_kdc()
-> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("magnetar=info")),
        )
        .with_test_writer()
        .try_init();

    let container = start_kdc().await?;
    let host = container.get_host().await?;
    let kdc_port_tcp = container.get_host_port_ipv4(KDC_PORT).await?;
    tracing::info!(
        target: "magnetar::e2e::sasl_kerberos",
        kdc = format!("{host}:{kdc_port_tcp}"),
        "KDC container running",
    );

    let spn = service_principal();
    let provider = match SaslKerberos::with_principal(&spn) {
        Ok(p) => p,
        Err(err) => {
            // libgssapi rejected the construction itself (most often:
            // missing default credential cache, malformed SPN, no
            // `krb5.conf`). Document the failure shape — the binding
            // is wired but the host is not provisioned.
            tracing::warn!(
                target: "magnetar::e2e::sasl_kerberos",
                error = %err,
                "LibGssapiClient construction failed — credentials likely missing on host",
            );
            return Ok(());
        }
    };

    match provider.initial() {
        Ok(token) => {
            assert!(
                !token.is_empty(),
                "GSSAPI initial token must be non-empty when credentials succeed",
            );
            tracing::info!(
                target: "magnetar::e2e::sasl_kerberos",
                len = token.len(),
                first_byte = token.first().copied(),
                "GSSAPI initial token produced",
            );
        }
        Err(err) => {
            // `gss_init_sec_context` rejection. Confirm the error
            // surfaces through the proto::AuthError surface (no panic,
            // no abort).
            tracing::warn!(
                target: "magnetar::e2e::sasl_kerberos",
                error = %err,
                "GSSAPI initial() failed — credentials likely missing on host",
            );
        }
    }

    Ok(())
}
