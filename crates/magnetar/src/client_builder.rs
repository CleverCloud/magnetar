// SPDX-License-Identifier: Apache-2.0

//! [`ClientBuilder`] — extracted from `client.rs` so the central
//! façade module stays focused on the [`crate::PulsarClient`] surface
//! and the per-surface builder types ([`crate::ProducerBuilder`],
//! [`crate::ConsumerBuilder`], [`crate::ReaderBuilder`]) that still
//! live alongside it.
//!
//! Re-exported via `pub use client_builder::ClientBuilder` from the
//! façade `lib.rs` so existing call sites
//! (`magnetar::ClientBuilder::default()`, `PulsarClient::builder()`)
//! keep working unchanged.

use std::time::Duration;

use magnetar_runtime_tokio::Client;

use crate::client::{MemoryLimit, MemoryLimitPolicy, PulsarClient, PulsarError};

/// Result alias used inside this module, mirroring the one in
/// `client.rs`.
type Result<T, E = PulsarError> = std::result::Result<T, E>;

/// Builder for [`PulsarClient`].
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    service_url: Option<String>,
    service_url_provider: Option<std::sync::Arc<dyn magnetar_proto::ServiceUrlProvider>>,
    client_version: Option<String>,
    keepalive: Option<Duration>,
    operation_timeout: Option<Duration>,
    auth_method_name: Option<String>,
    auth_data: Option<bytes::Bytes>,
    auth_provider: Option<std::sync::Arc<dyn magnetar_proto::AuthProvider>>,
    tls_trust_certs_pem: Option<Vec<u8>>,
    tls_allow_insecure_connection: bool,
    tls_hostname_verification_enable: bool,
    default_max_message_size: Option<usize>,
    proxy_to_broker_url: Option<String>,
    supervisor: Option<magnetar_proto::SupervisorConfig>,
    memory_limit: Option<MemoryLimit>,
    dns_resolver: Option<std::sync::Arc<dyn magnetar_runtime_tokio::DnsResolver>>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            service_url: None,
            service_url_provider: None,
            client_version: None,
            keepalive: None,
            operation_timeout: None,
            auth_method_name: None,
            auth_data: None,
            auth_provider: None,
            tls_trust_certs_pem: None,
            tls_allow_insecure_connection: false,
            tls_hostname_verification_enable: true,
            default_max_message_size: None,
            proxy_to_broker_url: None,
            supervisor: None,
            memory_limit: None,
            dns_resolver: None,
        }
    }
}

impl ClientBuilder {
    /// Set the Pulsar service URL (`pulsar://` or `pulsar+ssl://`).
    #[must_use]
    pub fn service_url(mut self, url: impl Into<String>) -> Self {
        self.service_url = Some(url.into());
        self
    }

    /// Plug in a custom DNS resolver. Mirrors Java
    /// `ClientBuilder#dnsResolver`. Used on every connection attempt
    /// (initial + reconnect) instead of tokio's default
    /// [`tokio::net::lookup_host`]. Useful for service-mesh sidecar
    /// resolution, IPv4/IPv6 preference, pinning, etc.
    ///
    /// Default: tokio's built-in DNS via
    /// [`magnetar_runtime_tokio::TokioDnsResolver`].
    #[must_use]
    pub fn dns_resolver(
        mut self,
        resolver: std::sync::Arc<dyn magnetar_runtime_tokio::DnsResolver>,
    ) -> Self {
        self.dns_resolver = Some(resolver);
        self
    }

    /// Set the global publish memory budget for the client. Mirrors Java
    /// `ClientBuilder#memoryLimit(long, MemoryLimitPolicy)`. `bytes = 0`
    /// disables the limit (matches Java default).
    ///
    /// **Enforcement**: under `MemoryLimitPolicy::FailImmediately`, every
    /// `Producer::send` reserves the payload bytes against the budget via
    /// an `AtomicU64` CAS loop on `ConnectionShared::memory_used` BEFORE
    /// the payload reaches the sans-io state machine. Sends that would
    /// push past the limit are rejected synchronously with
    /// [`magnetar_runtime_tokio::ClientError::MemoryLimitExceeded`]. The
    /// reservation is released on `SendFut` completion (success or
    /// error) and on cancellation (via `Drop`).
    ///
    /// Under `MemoryLimitPolicy::ProducerBlock`, the send future parks
    /// on a `Notify`-based wait until the budget frees up — both engines
    /// (`TokioEngine`, `MoonpoolEngine<P>`) implement this policy; see
    /// [`docs/memory-limit.md`](https://github.com/FlorentinDUBOIS/magnetar/blob/main/docs/memory-limit.md).
    #[must_use]
    pub fn memory_limit(mut self, bytes: usize, policy: MemoryLimitPolicy) -> Self {
        self.memory_limit = Some(MemoryLimit { bytes, policy });
        self
    }

    /// Set a pluggable [`magnetar_proto::ServiceUrlProvider`] consulted on every
    /// (re)connection attempt. Mirrors Java
    /// `ClientBuilder#serviceUrlProvider(ServiceUrlProvider)` — lays the groundwork
    /// for PIP-121 cluster failover (`AutoClusterFailover` /
    /// `ControlledClusterFailover`). When set, the provider's
    /// `get_service_url()` is used at connect time; the unset form retains the
    /// legacy `service_url(...)` shortcut and is internally wrapped in a
    /// [`magnetar_proto::StaticServiceUrlProvider`] at build time.
    #[must_use]
    pub fn service_url_provider(
        mut self,
        provider: std::sync::Arc<dyn magnetar_proto::ServiceUrlProvider>,
    ) -> Self {
        self.service_url_provider = Some(provider);
        self
    }

    /// Override the advertised client version.
    #[must_use]
    pub fn client_version(mut self, version: impl Into<String>) -> Self {
        self.client_version = Some(version.into());
        self
    }

    /// Set the keep-alive (ping) interval.
    #[must_use]
    pub fn keepalive(mut self, dur: Duration) -> Self {
        self.keepalive = Some(dur);
        self
    }

    /// Set the operation timeout (lookup + send).
    #[must_use]
    pub fn operation_timeout(mut self, dur: Duration) -> Self {
        self.operation_timeout = Some(dur);
        self
    }

    /// Override the default `max_message_size` used as the chunking threshold when the
    /// broker does not advertise one on `CommandConnected`. The Pulsar default is 5 MiB;
    /// match the broker's configured `maxMessageSize` to avoid mis-sized chunks. Mirrors
    /// Java `ClientBuilder#maxMessageSize`.
    #[must_use]
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.default_max_message_size = Some(size);
        self
    }

    /// Set the proxy-to-broker URL for the binary proxy path. The connection then opens
    /// against the proxy with the broker URL stamped on the `CommandConnect.proxy_to_broker_url`
    /// field. Mirrors Java `ClientBuilder#proxyServiceUrl(... ProxyProtocol.SNI)`. Leave
    /// unset for direct broker connections.
    #[must_use]
    pub fn proxy_to_broker_url(mut self, url: impl Into<String>) -> Self {
        self.proxy_to_broker_url = Some(url.into());
        self
    }

    /// Enable the auto-reconnect supervisor with the supplied
    /// [`magnetar_proto::SupervisorConfig`]. When set, runtime engines wrap the driver
    /// loop in a [`magnetar_proto::Backoff`]-driven reconnect cycle so the connection
    /// survives transport failures. Without this knob the driver exits on the first
    /// I/O error (matches the pre-supervisor behavior). Mirrors Java's
    /// `PulsarClientImpl` reconnect loop.
    ///
    /// Note: pending in-flight producer/consumer requests issued before the drop
    /// surface a "session lost" outcome on the new connection; transparent
    /// re-subscription and producer reattachment across reconnects is a future
    /// enhancement layered on top of this scaffold.
    #[must_use]
    pub fn enable_reconnect(mut self, config: magnetar_proto::SupervisorConfig) -> Self {
        self.supervisor = Some(config);
        self
    }

    /// Use the supplied auth provider to populate the initial CONNECT auth data,
    /// and keep the provider for in-band `CommandAuthChallenge` refresh
    /// (PIP-30 / PIP-292).
    ///
    /// **BREAKING CHANGE**: the provider's [`magnetar_proto::AuthProvider::initial`]
    /// is now invoked inside [`Self::build`] and any error it returns
    /// surfaces through [`PulsarError::Config`] — the previous behaviour
    /// silently dropped the error via `.ok()`, which would have let an
    /// uncached `OAuth2` flow / a missing token file / an expired credential
    /// open an *anonymous* connection (CWE-287). Callers using a provider
    /// whose `initial()` returns `Err(AuthError::Invalid)` until an
    /// out-of-band warm-up runs (e.g. `OAuth2Provider::ensure_fresh`) MUST
    /// warm the provider before calling [`Self::build`].
    #[must_use]
    pub fn auth(mut self, provider: std::sync::Arc<dyn magnetar_proto::AuthProvider>) -> Self {
        self.auth_method_name = Some(provider.method().to_owned());
        // NOTE: we deliberately do NOT call `provider.initial()` here. The
        // previous `.ok()` swallowed errors and let an unwarmed provider
        // produce an anonymous connection. The fetch + error propagation
        // now lives in `build()`.
        self.auth_provider = Some(provider);
        self
    }

    /// Mirrors Java `ClientBuilder#tlsTrustCertsFilePath` (PEM-supplied
    /// equivalent — magnetar keeps the façade I/O-free, callers read the
    /// file themselves via `std::fs::read(path)?` and pass the bytes).
    /// Supplies a PEM-encoded chain (typically a self-signed CA used by
    /// the broker). When set, the connection's TLS handshake validates
    /// the broker against this chain INSTEAD OF the system trust
    /// store. Only honoured for `pulsar+ssl://` URLs.
    #[must_use]
    pub fn tls_trust_certs_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.tls_trust_certs_pem = Some(pem.into());
        self
    }

    /// Mirror of Java `ClientBuilder#tlsAllowInsecureConnection`. When `true`,
    /// the TLS handshake accepts any server certificate without verifying its
    /// trust chain — useful for local development against a self-signed broker
    /// or for CI / e2e against an ephemeral container. **Insecure for
    /// production**: the client cannot tell a real broker from a MITM.
    ///
    /// Default: `false`. Only honoured for `pulsar+ssl://` URLs. Overrides any
    /// `tls_trust_certs_pem` chain when set.
    #[must_use]
    pub fn tls_allow_insecure_connection(mut self, on: bool) -> Self {
        self.tls_allow_insecure_connection = on;
        self
    }

    /// Mirror of Java `ClientBuilder#enableTlsHostnameVerification`. When
    /// `true` (the default), the handshake additionally checks the server
    /// certificate's CN / SAN matches the broker hostname from the URL. When
    /// `false`, the chain is still verified but the hostname mismatch is
    /// tolerated.
    ///
    /// Default: `true` (matches Java's secure default). When
    /// [`Self::tls_allow_insecure_connection`] is `true` this flag is moot —
    /// the verifier already accepts everything.
    ///
    /// **Note**: today only the "off + insecure both true" combination is
    /// runtime-enforced via [`magnetar_runtime_tokio::insecure_tls_config`].
    /// A hostname-only-skip verifier (chain on, hostname off) is a planned
    /// follow-up; passing `false` without also enabling
    /// `tls_allow_insecure_connection` is currently treated as the default
    /// (hostname verification stays on).
    #[must_use]
    pub fn tls_hostname_verification_enable(mut self, on: bool) -> Self {
        self.tls_hostname_verification_enable = on;
        self
    }

    /// Build and connect the client.
    ///
    /// # Errors
    /// Returns [`PulsarError::Config`] if the service URL is missing, or
    /// [`PulsarError::Client`] if the underlying tokio engine fails to
    /// connect.
    // The function is a flat config-translation: tls flavour cases on top, then config field
    // copies, then the connect-flavour dispatch. Inlined for readability — each branch is
    // straight-line and the dispatch is easier to follow without an extracted helper that
    // would have to forward every config field anyway.
    #[allow(clippy::too_many_lines)]
    pub async fn build(self) -> Result<PulsarClient> {
        let service_url = match (&self.service_url_provider, &self.service_url) {
            (Some(provider), _) => provider.get_service_url(),
            (None, Some(url)) => url.clone(),
            (None, None) => {
                return Err(PulsarError::Config(
                    "service_url or service_url_provider is required".to_owned(),
                ));
            }
        };
        let mut config = magnetar_proto::conn::ConnectionConfig::default();
        if let Some(v) = self.client_version {
            config.client_version = v;
        }
        if let Some(d) = self.keepalive {
            config.keepalive_interval = d;
        }
        if let Some(d) = self.operation_timeout {
            config.operation_timeout = d;
        }
        if let Some(s) = self.default_max_message_size {
            config.default_max_message_size = s;
        }
        if let Some(url) = self.proxy_to_broker_url {
            config.proxy_to_broker_url = Some(url);
        }
        if let Some(sv) = self.supervisor {
            config.supervisor = Some(sv);
        }
        // Java `ClientBuilder#memoryLimit` — wire the configured budget into the runtime so
        // `Producer::send` reserves payload bytes against `ConnectionShared::memory_limit_bytes`
        // before queueing. Both `FailImmediately` and `ProducerBlock` are honored by the
        // tokio and moonpool engines (the latter parks the send future on a `Notify` wait
        // until the budget frees up).
        if let Some(limit) = self.memory_limit {
            // Cast saturates rather than truncates so a 64-bit limit on a 32-bit usize host
            // (effectively impossible — magnetar requires 64-bit pointers — but cheap to
            // future-proof) stays correct.
            config.memory_limit_bytes = limit.bytes as u64;
        }
        if let Some(name) = self.auth_method_name {
            config.auth_method_name = name;
        }
        // BREAKING CHANGE: surface the provider's `initial()` failure here
        // rather than silently dropping it via `.ok()` in `auth(...)`. A
        // missing token file or an unwarmed OAuth2 cache used to slip
        // through and produce an anonymous CONNECT (CWE-287). The
        // direct-bytes `self.auth_data` set via internal call sites still
        // wins when present (matches the prior precedence).
        if let Some(data) = self.auth_data {
            config.auth_data = Some(data);
        } else if let Some(provider) = self.auth_provider.as_ref() {
            let bytes = provider.initial().map_err(|err| {
                PulsarError::Config(format!(
                    "auth provider initial() failed; cannot open authenticated connection: {err}"
                ))
            })?;
            config.auth_data = Some(bytes);
        }
        // Java `ClientBuilder#dnsResolver` — when configured, every reconnect (including the
        // initial dial) routes through `provider.resolve(host, port)` via
        // `Client::connect_with_resolver_and_provider`. When unset, the runtime falls back to
        // tokio's built-in `lookup_host` (and we can keep using the lighter `connect_auth`
        // shortcut when none of TLS / provider / resolver is configured).
        let inner = if self.tls_allow_insecure_connection {
            let parsed = magnetar_runtime_tokio::ParsedUrl::parse(&service_url)?;
            let tls_config = match parsed.scheme {
                magnetar_runtime_tokio::Scheme::Tls => {
                    Some(magnetar_runtime_tokio::insecure_tls_config())
                }
                magnetar_runtime_tokio::Scheme::Plain => None,
            };
            Client::connect_with_resolver_and_provider(
                parsed,
                tls_config,
                config,
                self.auth_provider,
                self.service_url_provider,
                self.dns_resolver,
            )
            .await?
        } else if let Some(pem) = self.tls_trust_certs_pem {
            let parsed = magnetar_runtime_tokio::ParsedUrl::parse(&service_url)?;
            let tls_config = match parsed.scheme {
                magnetar_runtime_tokio::Scheme::Tls => {
                    // Java parity: `enableTlsHostnameVerification(false)` paired with a
                    // PEM trust store keeps the chain check but skips the hostname match.
                    if self.tls_hostname_verification_enable {
                        Some(Client::tls_config_from_pem(&pem)?)
                    } else {
                        Some(magnetar_runtime_tokio::tls_config_no_hostname(&pem)?)
                    }
                }
                magnetar_runtime_tokio::Scheme::Plain => None,
            };
            Client::connect_with_resolver_and_provider(
                parsed,
                tls_config,
                config,
                self.auth_provider,
                self.service_url_provider,
                self.dns_resolver,
            )
            .await?
        } else if self.service_url_provider.is_some() || self.dns_resolver.is_some() {
            // Provider OR resolver configured but no explicit TLS / PEM. Go through the
            // provider+resolver-aware path so PIP-121 rotation AND custom DNS work on
            // reconnect — `connect_auth` doesn't accept either arg.
            let parsed = magnetar_runtime_tokio::ParsedUrl::parse(&service_url)?;
            let tls_config = match parsed.scheme {
                magnetar_runtime_tokio::Scheme::Tls => {
                    Some(magnetar_runtime_tokio::default_tls_config()?)
                }
                magnetar_runtime_tokio::Scheme::Plain => None,
            };
            Client::connect_with_resolver_and_provider(
                parsed,
                tls_config,
                config,
                self.auth_provider,
                self.service_url_provider,
                self.dns_resolver,
            )
            .await?
        } else {
            Client::connect_auth(&service_url, config, self.auth_provider).await?
        };
        Ok(PulsarClient {
            inner,
            memory_limit: self.memory_limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use magnetar_proto::{AuthError, AuthProvider};

    use super::ClientBuilder;
    use crate::PulsarError;

    /// Stub provider whose `initial()` returns `Err(AuthError::Invalid)`.
    /// Models an unwarmed `OAuth2` cache, a missing token file, or any other
    /// provider whose credential-fetch failed.
    #[derive(Debug)]
    struct FailingProvider;

    impl AuthProvider for FailingProvider {
        fn method(&self) -> &str {
            "token"
        }
        fn initial(&self) -> Result<Bytes, AuthError> {
            Err(AuthError::Invalid("forced failure (test)".to_owned()))
        }
    }

    /// BREAKING CHANGE regression (F6, CWE-287): `ClientBuilder::auth(...)`
    /// used to call `provider.initial().ok()`, silently dropping the error
    /// and leaving `auth_data = None`. The resulting CONNECT carried no
    /// credentials and the broker happily opened an *anonymous* session
    /// when its auth plugin allowed it — a textbook authentication-bypass
    /// vector when the provider is the only thing standing between the
    /// caller and an anonymous connection.
    ///
    /// The fix defers `provider.initial()` to `build()` and surfaces the
    /// failure through `PulsarError::Config`. This test pins that contract:
    /// no anonymous fallback, no broker dial, just an early `Err`.
    #[tokio::test(flavor = "current_thread")]
    async fn build_propagates_auth_provider_initial_error() {
        let provider = std::sync::Arc::new(FailingProvider);
        let result = ClientBuilder::default()
            // Localhost target is fine — `build()` must surface the auth
            // error BEFORE the dial, so no listener is required.
            .service_url("pulsar://127.0.0.1:1")
            .auth(provider)
            .build()
            .await;
        let err = result.expect_err(
            "build() must surface auth provider initial() error, not silently \
             fall back to an anonymous CONNECT (CWE-287)",
        );
        match err {
            PulsarError::Config(msg) => {
                assert!(
                    msg.contains("auth provider initial()"),
                    "error must point at the auth path: {msg}"
                );
                assert!(
                    msg.contains("forced failure (test)"),
                    "error must propagate the provider's message: {msg}"
                );
            }
            other => {
                panic!("expected PulsarError::Config carrying the auth failure, got: {other:?}")
            }
        }
    }
}
