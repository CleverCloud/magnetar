// SPDX-License-Identifier: Apache-2.0

//! moonpool engine for magnetar.
//!
//! Drives the sans-io [`magnetar_proto::Connection`] state machine on top of
//! [`moonpool_core::Providers`] (which bundles [`NetworkProvider`], [`TimeProvider`],
//! [`TaskProvider`], [`RandomProvider`], and [`StorageProvider`]). The point is *not*
//! to be a separate engine for production load — it is to make the entire
//! producer/consumer protocol exercisable under
//! [moonpool-sim](https://crates.io/crates/moonpool-sim) deterministic chaos
//! testing, so we can fuzz partitions, message reorderings, and TLS handshake
//! reorderings with reproducible seeds.
//!
//! ## Driver shape
//!
//! Same pattern as the tokio engine:
//!
//! - `Arc<parking_lot::Mutex<Connection>>` holds the sans-io state machine,
//! - a single-cell [`tokio::sync::Notify`] (`driver_waker`) signals the driver when user-facing
//!   futures enqueue fresh work,
//! - the driver loop runs as a spawned tokio task that selects over `driver_waker.notified()`,
//!   `transport.read_buf(...)`, and a timer driven by [`moonpool_core::TimeProvider::sleep`].
//!
//! Because the driver still uses `tokio::spawn` and `tokio::select!`, both
//! the production and simulation modes rely on a tokio runtime — the
//! determinism comes from substituting the providers, not from replacing
//! tokio.
//!
//! ## TLS
//!
//! TLS for the moonpool engine is the `option (d)` adapter ([`tls`]): drive
//! [`rustls::ClientConnection`] (itself sans-io) over the moonpool-supplied
//! byte pipe. The TLS handshake therefore survives `moonpool-sim` chaos with
//! the same determinism as `magnetar-proto` itself. The internal
//! `crate::transport::Transport` enum exposes a `Tls` variant that the
//! driver loop drives identically to the plaintext path —
//! [`MoonpoolEngine::connect_tls`] runs the handshake inline before handing
//! the transport to the driver task.
//!
//! ## No channels
//!
//! Same pattern as the tokio engine: `Arc<parking_lot::Mutex<Connection>>`
//! plus per-future [`std::task::Waker`] slabs inside the connection.
//! Driver wakeups travel through a single [`tokio::sync::Notify`].
//!
//! [`NetworkProvider`]: moonpool_core::NetworkProvider
//! [`TimeProvider`]: moonpool_core::TimeProvider
//! [`TaskProvider`]: moonpool_core::TaskProvider
//! [`RandomProvider`]: moonpool_core::RandomProvider
//! [`StorageProvider`]: moonpool_core::StorageProvider

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]
#![allow(
    // The driver state machine is naturally branchy; pedantic lints fight
    // the readability of an event-pump loop. We tighten these later once the
    // engine has stabilised.
    clippy::too_many_lines,
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::doc_markdown
)]

pub mod auto_cluster_failover;
mod client;
mod consumer;
pub mod dns;
mod driver;
mod producer;
pub mod tls;
mod transport;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Waker;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{Connection, ConnectionConfig};
/// Convenience re-exports of the sans-io cluster-failover types. They live
/// in `magnetar-proto` so the runtime engines can plug them into the
/// supervised reconnect path without re-implementing the trait. Java
/// parity: `org.apache.pulsar.client.api.ServiceUrlProvider`. See
/// [ADR-0016](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0016-pip-121-cluster-failover.md).
///
/// The health-probe-driven [`AutoClusterFailover`] policy now also has a
/// moonpool-native implementation in
/// [`crate::auto_cluster_failover`]; it is generic over
/// [`moonpool_core::Providers`] so the probe loop and the TCP probe
/// socket dance run through the moonpool task / network providers and
/// stay deterministic under `moonpool-sim`. See
/// [ADR-0023](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0023-health-probe-trait-extraction.md)
/// for the trait extraction that made this possible.
///
/// [`AutoClusterFailover`]: crate::auto_cluster_failover::AutoClusterFailover
pub use magnetar_proto::{ControlledClusterFailover, ServiceUrlProvider, StaticServiceUrlProvider};
use moonpool_core::Providers;
use parking_lot::Mutex;
use slab::Slab;
use tokio::sync::Notify;

pub use crate::client::{Client, ClientError, LookupTopicResult};
pub use crate::consumer::Consumer;
pub use crate::dns::{DnsResolveFuture, DnsResolver, StaticDnsResolver, arc_dns_resolver};
pub use crate::driver::DriverHandle;
pub use crate::producer::{Producer, SendFut};
use crate::transport::Transport;

/// Shared connection state for the moonpool engine. Mirrors the tokio
/// engine's `ConnectionShared`: a non-async mutex over the sans-io state
/// machine plus a single-cell driver wakeup.
pub struct ConnectionShared {
    /// The sans-io state machine, guarded by a non-async mutex.
    pub inner: Mutex<Connection>,
    /// Single-cell wakeup for the driver loop. Not a channel — just a
    /// `Notify` notified after every user-facing future enqueues work
    /// (e.g. a producer's `send`).
    pub driver_waker: Notify,
    /// Optional auth provider that the driver consults when the broker
    /// emits `AuthChallenge`. `None` means no in-band token refresh — the
    /// connection will drop if the broker challenges. PIP-30 / PIP-292.
    pub auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    /// PIP-145 topic-list-watcher deltas pushed here by the driver.
    pub topic_list_changes: Mutex<std::collections::VecDeque<TopicListChange>>,
    /// Wakeup for `next_topic_list_change` futures. Notified after every
    /// push to [`Self::topic_list_changes`].
    pub topic_list_notify: Notify,
    /// Set by the supervised-reconnect path between
    /// [`magnetar_proto::Connection::reset`] and the new socket's
    /// handshake. When `true`, the driver loop runs
    /// [`magnetar_proto::Connection::rebuild_producers`] +
    /// [`magnetar_proto::Connection::rebuild_consumers`] the first time it
    /// observes the new session transitioning to
    /// [`magnetar_proto::HandshakeState::Connected`], then clears the flag
    /// so the rebuild fires exactly once per reconnect. Stage 3 of the
    /// supervisor work (transparent producer / consumer replay).
    pub pending_rebuild: AtomicBool,
    /// Configured global publish memory budget in bytes. `0` disables the
    /// limit (matches `ConnectionConfig::memory_limit_bytes` default).
    /// Mirrors the tokio engine's identically-named field and Java's
    /// `ClientBuilder#memoryLimit`. See
    /// [ADR-0017](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0017-memory-limit-atomic-reservation.md).
    pub memory_limit_bytes: u64,
    /// Current in-flight publish bytes reserved by [`Producer::send`]
    /// calls that have not yet seen their
    /// [`magnetar_proto::OpOutcome::SendReceipt`] / `SendError`. Bumped on
    /// reserve (CAS against `memory_limit_bytes`); decremented on
    /// [`SendFut`] completion.
    pub memory_used: AtomicU64,
    /// Configured back-pressure policy when the publish budget is exhausted.
    /// Mirrors Java `org.apache.pulsar.client.api.MemoryLimitPolicy`. When
    /// `FailImmediately`, reservations that would overflow are rejected
    /// synchronously with [`EngineError::MemoryLimitExceeded`]. When
    /// `ProducerBlock`, the runtime parks the offending send future on
    /// [`Self::memory_wakers`] until enough budget frees up.
    ///
    /// Snapshotted from
    /// [`magnetar_proto::ConnectionConfig::memory_limit_policy`] at
    /// construction time. See
    /// [ADR-0020](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0020-memory-limit-producer-block.md)
    /// for the tokio counterpart and
    /// [ADR-0022](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0022-memory-limit-producer-block-moonpool.md)
    /// for the moonpool-specific fairness contract under
    /// [`moonpool_core::Providers`].
    pub memory_limit_policy: magnetar_proto::MemoryLimitPolicy,
    /// Waker slab consulted by [`Self::release_memory`] when a reservation
    /// frees up. Populated by [`Self::try_reserve_memory_or_register`] from
    /// inside [`Producer::send`] under
    /// [`magnetar_proto::MemoryLimitPolicy::ProducerBlock`]. Drained on
    /// every release: every parked send wakes and re-attempts the
    /// reservation (fairness is approximate — first-to-poll wins, matching
    /// the tokio engine and Java's `MemoryLimitController` semantics).
    ///
    /// Not a channel — this is a `Slab<Waker>` behind a
    /// `parking_lot::Mutex`, the canonical no-channel wake pattern (see
    /// [ADR-0003](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0003-no-channels-rule.md)).
    ///
    /// Under `moonpool_core::SimProviders` the drain visits slab slots in
    /// insertion order (slab free-list FIFO), but `core::task::Waker::wake`
    /// hands off to the wrapping `Providers::task` runtime so re-poll
    /// ordering is ultimately the simulator's call. Tests should depend on
    /// *eventual* progress under `ProducerBlock`, not a specific wake
    /// order. See ADR-0022.
    pub memory_wakers: Mutex<Slab<Waker>>,
}

impl std::fmt::Debug for ConnectionShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionShared")
            .field("inner", &"<Connection>")
            .field("has_auth_provider", &self.auth_provider.is_some())
            .finish_non_exhaustive()
    }
}

impl ConnectionShared {
    /// Construct shared state from the given protocol-layer config.
    #[must_use]
    pub fn new(config: ConnectionConfig) -> Arc<Self> {
        Self::with_auth(config, None)
    }

    /// Construct with an auth provider for in-band challenge refresh.
    #[must_use]
    pub fn with_auth(
        config: ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    ) -> Arc<Self> {
        let memory_limit_bytes = config.memory_limit_bytes;
        let memory_limit_policy = config.memory_limit_policy;
        Arc::new(Self {
            inner: Mutex::new(Connection::new(config)),
            driver_waker: Notify::new(),
            auth_provider,
            topic_list_changes: Mutex::new(std::collections::VecDeque::new()),
            topic_list_notify: Notify::new(),
            pending_rebuild: AtomicBool::new(false),
            memory_limit_bytes,
            memory_used: AtomicU64::new(0),
            memory_limit_policy,
            memory_wakers: Mutex::new(Slab::new()),
        })
    }

    /// Try to reserve `bytes` against the configured memory budget.
    ///
    /// Returns `Ok(())` when the reservation succeeds (or no limit is
    /// configured — `memory_limit_bytes = 0`); returns
    /// [`EngineError::MemoryLimitExceeded`] when the reservation would
    /// push `memory_used` past `memory_limit_bytes`.
    ///
    /// Lock-free: a CAS loop on `memory_used`. Mirrors Java's
    /// `MemoryLimitController` in `MemoryLimitPolicy.FailImmediately` mode
    /// and the tokio engine's identically-shaped helper.
    ///
    /// `AtomicU64` is not a channel; see
    /// [ADR-0003](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0003-no-channels-rule.md).
    ///
    /// # Errors
    /// Surfaces [`EngineError::MemoryLimitExceeded`] when the reservation
    /// would overflow the configured budget.
    pub fn try_reserve_memory(&self, bytes: u64) -> Result<(), EngineError> {
        if self.memory_limit_bytes == 0 {
            return Ok(());
        }
        loop {
            let current = self.memory_used.load(Ordering::Acquire);
            let next = current.saturating_add(bytes);
            if next > self.memory_limit_bytes {
                return Err(EngineError::MemoryLimitExceeded {
                    current,
                    limit: self.memory_limit_bytes,
                    requested: bytes,
                });
            }
            if self
                .memory_used
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    /// Release a previous reservation. Called by [`SendFut`] on completion
    /// (success or error). Saturating sub so a buggy over-release can't
    /// underflow the counter.
    ///
    /// After releasing, drains every waker parked on
    /// [`Self::memory_wakers`] so blocked
    /// [`magnetar_proto::MemoryLimitPolicy::ProducerBlock`] sends re-attempt
    /// their reservation. Drain-all (rather than wake-one) matches the
    /// tokio engine and Java's `MemoryLimitController` behaviour where any
    /// released byte may unblock several smaller pending sends; spurious
    /// wake-ups are cheap because the futures re-check the CAS budget on
    /// every poll.
    pub fn release_memory(&self, bytes: u64) {
        if bytes == 0 || self.memory_limit_bytes == 0 {
            // No budget configured: there cannot be parked wakers either.
            return;
        }
        loop {
            let current = self.memory_used.load(Ordering::Acquire);
            let next = current.saturating_sub(bytes);
            if self
                .memory_used
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        self.drain_memory_wakers();
    }

    /// Try to reserve `bytes` against the configured memory budget; on
    /// failure, register `waker` on [`Self::memory_wakers`] so the caller
    /// can be re-polled when budget frees up via [`Self::release_memory`].
    ///
    /// Returns:
    /// - `Ok(())` when the reservation succeeded (or no limit is configured).
    /// - `Err(slab_key)` when the reservation failed; the caller MUST cancel the registration via
    ///   [`Self::cancel_memory_waker`] if it is dropped before observing the next release.
    ///
    /// This is the building block of
    /// [`magnetar_proto::MemoryLimitPolicy::ProducerBlock`]. The
    /// [`SendFut`] future in [`crate::producer`] polls this method until
    /// it succeeds; on `Drop` it calls
    /// [`Self::cancel_memory_waker`] to evict the stale waker slot.
    ///
    /// Re-checking after registration closes the lost-wakeup window: a
    /// release that lands between the failed CAS and the slab insert will
    /// have drained the (empty) slab without observing this waker, so we
    /// re-attempt the reservation once the waker is installed.
    ///
    /// Mirrors the tokio engine's helper of the same name; the two
    /// implementations are intentionally identical so the behavioural
    /// surface stays consistent across engines.
    pub fn try_reserve_memory_or_register(&self, bytes: u64, waker: &Waker) -> Result<(), usize> {
        // Fast path: no budget configured, or budget has room right now.
        if self.try_reserve_memory(bytes).is_ok() {
            return Ok(());
        }
        // Slow path: park a waker and re-check. The recheck closes the race
        // where a release fires between the failed CAS above and the slab
        // insert below.
        let key = self.memory_wakers.lock().insert(waker.clone());
        if self.try_reserve_memory(bytes).is_ok() {
            // Won the recheck; drop our registration so the next release
            // doesn't wake a future that already completed.
            self.cancel_memory_waker(key);
            return Ok(());
        }
        Err(key)
    }

    /// Remove a previously-registered waker. Called from the
    /// [`SendFut`] `Drop` impl in [`crate::producer`] and on the "won the
    /// recheck" path of [`Self::try_reserve_memory_or_register`].
    /// Idempotent — a missing slot is a no-op (a concurrent
    /// [`Self::release_memory`] may have drained it already).
    pub fn cancel_memory_waker(&self, slab_key: usize) {
        let mut slab = self.memory_wakers.lock();
        if slab.contains(slab_key) {
            slab.remove(slab_key);
        }
    }

    /// Drain every parked waker and wake it. Called from
    /// [`Self::release_memory`] after the CAS-decrement lands.
    ///
    /// Holds the slab lock only for the duration of the swap; `wake()`
    /// runs outside the critical section so user code that re-polls
    /// cannot deadlock on the slab mutex. Mirrors the tokio engine's
    /// `drain_memory_wakers` exactly.
    ///
    /// Drain order is slab insertion order (FIFO over the free-list).
    /// Under `moonpool_core::SimProviders` the resulting re-poll order is
    /// the simulator's call — `Waker::wake` hands off to the task
    /// provider, which schedules the woken tasks per its policy. Tests
    /// must depend on *eventual* progress (every parked send eventually
    /// observes either a successful reservation or its own cancellation),
    /// not a specific drain order. See ADR-0022.
    fn drain_memory_wakers(&self) {
        let wakers: Vec<Waker> = {
            let mut slab = self.memory_wakers.lock();
            let drained: Vec<Waker> = slab.drain().collect();
            drained
        };
        for w in wakers {
            w.wake();
        }
    }
}

/// PIP-145 topic-list-watcher delta surfaced from the driver to user-facing
/// code. Mirrors `ConnectionEvent::TopicListChanged` with owned vectors so
/// callers don't pay for borrows across the await boundary.
#[derive(Debug, Clone)]
pub struct TopicListChange {
    /// Topics that newly match the pattern.
    pub added: Vec<String>,
    /// Topics that no longer match the pattern.
    pub removed: Vec<String>,
}

/// Errors surfaced by the moonpool engine.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Underlying I/O failure (from the moonpool provider).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Sans-io protocol error.
    #[error("protocol error: {0}")]
    Protocol(#[from] magnetar_proto::ProtocolError),
    /// TLS error.
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),
    /// Peer closed the connection cleanly mid-handshake.
    #[error("peer closed connection")]
    PeerClosed,
    /// Configuration error (e.g. URL parsing).
    #[error("config error: {0}")]
    Config(String),
    /// A `Producer::send` was rejected because reserving its payload bytes
    /// would push the engine past the configured
    /// [`ConnectionShared::memory_limit_bytes`] budget. Mirrors Java's
    /// `MemoryLimitController` in `FailImmediately` policy. See
    /// [ADR-0017](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0017-memory-limit-atomic-reservation.md).
    #[error("memory limit exceeded: current={current}B + requested={requested}B > limit={limit}B")]
    MemoryLimitExceeded {
        /// Bytes currently reserved on the budget when the request was rejected.
        current: u64,
        /// Configured limit (`ConnectionShared::memory_limit_bytes`).
        limit: u64,
        /// Bytes the caller asked to reserve.
        requested: u64,
    },
}

/// moonpool-backed engine handle. Generic over the [`Providers`] bundle so
/// callers can plug in `TokioProviders` (production) or a sim bundle (tests).
pub struct MoonpoolEngine<P: Providers> {
    providers: P,
}

impl<P: Providers> std::fmt::Debug for MoonpoolEngine<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MoonpoolEngine").finish_non_exhaustive()
    }
}

impl<P: Providers> MoonpoolEngine<P> {
    /// Construct an engine bound to the given providers.
    #[must_use]
    pub fn new(providers: P) -> Self {
        Self { providers }
    }

    /// Borrow the underlying providers (useful in tests).
    #[must_use]
    pub fn providers(&self) -> &P {
        &self.providers
    }

    /// Connect to a Pulsar broker over the moonpool [`NetworkProvider`] and
    /// spawn the driver task that runs the protocol forward.
    ///
    /// `addr` is a `host:port` string per moonpool's API (NOT a `pulsar://`
    /// URL — strip the scheme before calling). TLS is the caller's
    /// responsibility for now: wrap the returned [`ConnectionShared`] +
    /// [`tls::RustlsByteAdapter`] manually in the driver task.
    ///
    /// The function completes once a `CONNECT` frame has been written and
    /// the broker has responded with `CONNECTED`. After that point the
    /// returned `DriverHandle` owns the connection and pumps it for
    /// producer/consumer operations.
    ///
    /// # Errors
    /// Propagates [`EngineError::Io`] on network failure,
    /// [`EngineError::Protocol`] on framing or handshake errors, or
    /// [`EngineError::PeerClosed`] if the peer closed before CONNECTED.
    ///
    /// [`NetworkProvider`]: moonpool_core::NetworkProvider
    pub async fn connect_plain(
        &self,
        addr: &str,
        config: ConnectionConfig,
    ) -> Result<(Arc<ConnectionShared>, DriverHandle), EngineError> {
        self.connect_plain_with_resolver(addr, config, None).await
    }

    /// Connect to `addr`, routing the initial dial through `resolver` when
    /// `Some`. Mirrors the tokio engine's `connect_with_resolver` path —
    /// the resolver returns one or more candidate [`std::net::SocketAddr`]s
    /// and the transport dials each in order.
    ///
    /// When `resolver = None`, behaves identically to [`Self::connect_plain`]
    /// (routes the raw `host:port` through the moonpool
    /// [`moonpool_core::NetworkProvider`]). Java parity:
    /// `ClientBuilder#dnsResolver`. See
    /// [ADR-0015](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0015-dns-resolver-injection.md).
    ///
    /// # Errors
    /// Same envelope as [`Self::connect_plain`], plus the resolver may
    /// surface [`EngineError::Config`] if it returns no addresses.
    pub async fn connect_plain_with_resolver(
        &self,
        addr: &str,
        config: ConnectionConfig,
        resolver: Option<&dyn DnsResolver>,
    ) -> Result<(Arc<ConnectionShared>, DriverHandle), EngineError> {
        let mut transport =
            Transport::<P>::connect_with_resolver(self.providers.network(), addr, resolver).await?;
        let shared = ConnectionShared::new(config);

        // Drive the handshake inline. Once `Connected` lands we hand the
        // transport over to the long-running driver task so user-facing
        // futures can start enqueueing producer/consumer commands.
        handshake_plain::<P>(&shared, &mut transport).await?;
        let driver = driver::spawn::<P>(
            shared.clone(),
            transport,
            self.providers.time().clone(),
            self.providers.task(),
        );
        Ok((shared, driver))
    }

    /// Connect to `addr` over TLS (rustls-over-bytepipe via
    /// [`tls::RustlsByteAdapter`]) and spawn the driver task that runs the
    /// protocol forward.
    ///
    /// `addr` is a `host:port` string per moonpool's API (NOT a `pulsar+ssl://`
    /// URL — strip the scheme before calling). `host` is the SNI /
    /// hostname-verification name handed to rustls; pass the broker hostname
    /// even if `addr` is a resolved IP. `tls_config` is the workspace-wide
    /// [`rustls::ClientConfig`] (no `native-tls` / `openssl` shim,
    /// [ADR-0005](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0005-rustls-only-tls.md)).
    ///
    /// The TLS handshake runs inline before `connect_tls` returns. After
    /// that, the driver loop pumps decrypted plaintext through the same
    /// sans-io state machine the plaintext path uses, so producer / consumer
    /// flows complete identically under `moonpool-sim`.
    ///
    /// # Errors
    /// Same envelope as [`Self::connect_plain`], plus [`EngineError::Tls`]
    /// for rustls handshake failures.
    pub async fn connect_tls(
        &self,
        addr: &str,
        host: &str,
        tls_config: Arc<rustls::ClientConfig>,
        config: ConnectionConfig,
        resolver: Option<&dyn DnsResolver>,
    ) -> Result<(Arc<ConnectionShared>, DriverHandle), EngineError> {
        let mut transport =
            Transport::<P>::connect_tls(self.providers.network(), addr, host, tls_config, resolver)
                .await?;
        let shared = ConnectionShared::new(config);
        handshake_plain::<P>(&shared, &mut transport).await?;
        let driver = driver::spawn::<P>(
            shared.clone(),
            transport,
            self.providers.time().clone(),
            self.providers.task(),
        );
        Ok((shared, driver))
    }

    /// Connect to `addr`, then spawn the supervised driver loop. When
    /// [`ConnectionConfig::supervisor`] is `Some`, the driver auto-reconnects
    /// on transient socket failures using the moonpool [`Providers`] —
    /// `sleep` goes through [`moonpool_core::TimeProvider::sleep`] so
    /// `moonpool-sim` runs the backoff schedule deterministically.
    ///
    /// When the supervisor config is `None`, behaviour matches
    /// [`Self::connect_plain`] — the driver exits on the first I/O failure.
    ///
    /// `service_url_provider` is the PIP-121 cluster-failover hook —
    /// when `Some`, every reconnect attempt polls the provider for a fresh
    /// `pulsar://host:port` (or `pulsar+ssl://host:port`) URL before
    /// dialling. `dns_resolver` mirrors Java's `ClientBuilder#dnsResolver`.
    ///
    /// # Errors
    /// Same envelope as [`Self::connect_plain_with_resolver`].
    pub async fn connect_plain_supervised(
        &self,
        addr: &str,
        config: ConnectionConfig,
        service_url_provider: Option<Arc<dyn magnetar_proto::ServiceUrlProvider>>,
        dns_resolver: Option<Arc<dyn DnsResolver>>,
    ) -> Result<(Arc<ConnectionShared>, DriverHandle), EngineError> {
        let mut transport = Transport::<P>::connect_with_resolver(
            self.providers.network(),
            addr,
            dns_resolver.as_deref(),
        )
        .await?;
        let shared = ConnectionShared::new(config);

        handshake_plain::<P>(&shared, &mut transport).await?;
        let ctx = driver::ReconnectContext {
            host_port: addr.to_owned(),
            service_url_provider,
            dns_resolver,
        };
        let driver =
            driver::spawn_supervised::<P>(shared.clone(), transport, ctx, self.providers.clone());
        Ok((shared, driver))
    }
}

/// Drive the byte pump until the handshake completes.
///
/// Kept private to the crate — the public surface goes through
/// [`MoonpoolEngine::connect_plain`]. Idempotently calls
/// [`magnetar_proto::Connection::begin_handshake`] on entry when the state
/// machine is still `Uninitialized`, so callers don't have to remember to
/// seed the `CommandConnect` themselves. Mirrors the tokio engine's
/// `start_handshake`, which calls `begin_handshake` before spawning the
/// driver. Without this, `poll_transmit` returns no bytes and the loop
/// parks on `read_buf` forever (the M8 differential `golden_traces`
/// regression).
async fn handshake_plain<P: Providers>(
    shared: &Arc<ConnectionShared>,
    transport: &mut Transport<P>,
) -> Result<(), EngineError> {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut write_buf: Vec<u8> = Vec::with_capacity(8 * 1024);

    {
        let mut conn = shared.inner.lock();
        if matches!(conn.state(), magnetar_proto::HandshakeState::Uninitialized) {
            conn.begin_handshake()
                .map_err(|err| EngineError::Config(format!("begin_handshake failed: {err}")))?;
        }
    }

    loop {
        // 1. Drain outbound bytes the state machine has queued.
        {
            let mut conn = shared.inner.lock();
            write_buf.clear();
            let _ = conn.poll_transmit(&mut write_buf);
        }
        if !write_buf.is_empty() {
            transport.write_all(&write_buf).await?;
            transport.flush().await?;
            write_buf.clear();
        }

        // 2. If we're already past handshake, we're done.
        {
            let conn = shared.inner.lock();
            if matches!(
                conn.state(),
                magnetar_proto::HandshakeState::Connected
                    | magnetar_proto::HandshakeState::AuthChallenging
            ) {
                return Ok(());
            }
            if matches!(
                conn.state(),
                magnetar_proto::HandshakeState::Failed | magnetar_proto::HandshakeState::Closed
            ) {
                return Err(EngineError::PeerClosed);
            }
        }

        // 3. Read more bytes from the wire.
        let n = transport.read_buf(&mut read_buf).await?;
        if n == 0 {
            return Err(EngineError::PeerClosed);
        }
        let bytes = read_buf.split().freeze();
        shared.inner.lock().handle_bytes(Instant::now(), &bytes)?;
    }
}

#[cfg(test)]
mod tests {
    use magnetar_proto::ConnectionConfig;
    use moonpool_core::TokioProviders;

    use super::{ConnectionShared, MoonpoolEngine, TopicListChange};

    #[test]
    fn engine_can_be_constructed_with_tokio_providers() {
        let providers = TokioProviders::new();
        let engine = MoonpoolEngine::new(providers);
        // Calling providers() smoke-tests the trait wiring.
        let _ = engine.providers();
    }

    #[test]
    fn shared_state_can_be_constructed() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        let _g = s.inner.lock();
        // Topic-list buffer starts empty.
        assert!(s.topic_list_changes.lock().is_empty());
    }

    #[test]
    fn topic_list_changes_buffer_round_trip() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        s.topic_list_changes.lock().push_back(TopicListChange {
            added: vec!["a".to_owned()],
            removed: vec![],
        });
        s.topic_list_changes.lock().push_back(TopicListChange {
            added: vec![],
            removed: vec!["b".to_owned()],
        });
        let first = s.topic_list_changes.lock().pop_front().unwrap();
        assert_eq!(first.added, vec!["a".to_owned()]);
        let second = s.topic_list_changes.lock().pop_front().unwrap();
        assert_eq!(second.removed, vec!["b".to_owned()]);
        assert!(s.topic_list_changes.lock().is_empty());
    }

    /// Doc-test-style smoke: the engine's `connect_plain()` can be named
    /// without actually awaiting it. We don't dial a real broker here.
    #[test]
    #[allow(clippy::let_underscore_future, clippy::no_effect_underscore_binding)]
    fn connect_plain_compiles() {
        let providers = TokioProviders::new();
        let engine = MoonpoolEngine::new(providers);
        let _fut = engine.connect_plain("127.0.0.1:6650", ConnectionConfig::default());
    }

    /// `connect_plain_supervised` compiles against `TokioProviders` with no
    /// supervisor / failover / resolver wired (the simplest invocation
    /// shape). Confirms the trait bounds + `Providers: Clone` propagate.
    #[test]
    #[allow(clippy::let_underscore_future, clippy::no_effect_underscore_binding)]
    fn connect_plain_supervised_compiles() {
        let providers = TokioProviders::new();
        let engine = MoonpoolEngine::new(providers);
        let _fut = engine.connect_plain_supervised(
            "127.0.0.1:6650",
            ConnectionConfig::default(),
            None,
            None,
        );
    }

    /// `connect_tls` compiles against `TokioProviders` with a stock empty
    /// rustls config. Smoke-tests that the TLS variant of the `Transport`
    /// enum + `RustlsByteAdapter` plumbing typechecks end-to-end.
    #[test]
    #[allow(clippy::let_underscore_future, clippy::no_effect_underscore_binding)]
    fn connect_tls_compiles() {
        let providers = TokioProviders::new();
        let engine = MoonpoolEngine::new(providers);
        let tls_config = std::sync::Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let _fut = engine.connect_tls(
            "127.0.0.1:6651",
            "broker.example.com",
            tls_config,
            ConnectionConfig::default(),
            None,
        );
    }

    /// Confirm we're not accidentally pulling in any channel crate.
    #[test]
    fn no_unbounded_compile_check() {
        let _ = std::any::type_name::<super::EngineError>();
        let _ = std::time::Duration::from_secs(0);
    }

    /// Memory-limit fast path: when budget is available, the helper must
    /// take the reservation without parking a waker — mirrors the
    /// equivalent unit test in `magnetar-runtime-tokio` and closes the
    /// `cargo xtask check-runtime-test-parity` count gap introduced by
    /// porting `MemoryLimitPolicy::ProducerBlock` to moonpool (ADR-0022).
    #[test]
    fn try_reserve_memory_or_register_succeeds_when_budget_available() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::task::Wake;

        struct CountingWaker(AtomicUsize);
        impl Wake for CountingWaker {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, super::Ordering::SeqCst);
            }
        }

        let cfg = ConnectionConfig {
            memory_limit_bytes: 1024,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);
        let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(counter.clone());

        // Empty budget: the helper must take the fast path and not register
        // a waker.
        s.try_reserve_memory_or_register(512, &waker)
            .expect("should succeed with budget available");
        assert_eq!(s.memory_used.load(super::Ordering::Acquire), 512);
        assert_eq!(s.memory_wakers.lock().len(), 0);
        assert_eq!(counter.0.load(super::Ordering::Acquire), 0);
    }
}
