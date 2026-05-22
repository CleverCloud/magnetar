// SPDX-License-Identifier: Apache-2.0

//! Chaos scenario: an OAuth2-style token expires at virtual `t = 3600s`.
//! Driving `AuthProvider::initial` (the call the moonpool driver issues to
//! populate `CommandConnect.auth_data` and `CommandAuthResponse.auth_data`)
//! at `t = 3500s` returns the cached token; driving it again at
//! `t = 3601s` returns a refreshed token.
//!
//! Why this is moonpool territory: a `testcontainers` test would have to
//! run for an hour of real wall-clock time or stub the IDP — the first is
//! flaky, the second is a unit test in disguise. The clean way to pin the
//! refresh-edge contract is to drive a synthetic [`Clock`] forward and
//! observe the auth provider's behaviour.
//!
//! This test does **not** invoke the real
//! [`magnetar_auth_oauth2::ClientCredentialsFlow`] — that requires HTTP
//! traffic against an IDP. Instead it exercises the contract every clock-
//! driven `AuthProvider` must satisfy: when the cached token has expired
//! relative to the injected clock, the next `initial()` call returns fresh
//! bytes and the underlying clock is consulted exactly once per
//! refresh-or-hit cycle.
//!
//! The same contract is what `ClientCredentialsFlow::ensure_fresh` enforces
//! (see `magnetar-auth-oauth2/src/lib.rs`); pinning it here against a
//! synthetic `Clock` gives us bit-for-bit determinism with no IDP in the
//! loop.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use magnetar_proto::auth::{AuthError, AuthProvider};
use parking_lot::Mutex;

/// Virtual monotonic clock. Test code calls [`Self::advance`] to step the
/// clock forward without touching the host wall clock. Cheap to clone (Arc
/// bump).
#[derive(Debug, Clone)]
struct VirtualClock {
    inner: Arc<Mutex<Instant>>,
    /// Counter of `now()` calls — lets the test prove the auth layer
    /// consulted the clock and didn't fall back to `Instant::now`.
    calls: Arc<AtomicU64>,
}

impl VirtualClock {
    fn new(start: Instant) -> Self {
        Self {
            inner: Arc::new(Mutex::new(start)),
            calls: Arc::new(AtomicU64::new(0)),
        }
    }

    fn now(&self) -> Instant {
        self.calls.fetch_add(1, Ordering::Relaxed);
        *self.inner.lock()
    }

    fn advance(&self, by: Duration) {
        let mut guard = self.inner.lock();
        *guard += by;
    }

    fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
}

/// Clock-driven OAuth2-shaped auth provider. Mirrors the cache shape of
/// `magnetar_auth_oauth2::ClientCredentialsFlow` (a cached token tied to a
/// monotonic deadline) without the HTTP plumbing.
#[derive(Debug)]
struct VirtualOAuthProvider {
    clock: VirtualClock,
    ttl: Duration,
    /// Counter of successful refreshes — lets the test prove a second
    /// refresh fired across the virtual deadline.
    refreshes: Arc<AtomicU64>,
    /// Cached token; `(bytes, expires_at)`.
    cached: Mutex<Option<(Bytes, Instant)>>,
}

impl VirtualOAuthProvider {
    fn new(clock: VirtualClock, ttl: Duration) -> Self {
        Self {
            clock,
            ttl,
            refreshes: Arc::new(AtomicU64::new(0)),
            cached: Mutex::new(None),
        }
    }

    fn refresh_count(&self) -> u64 {
        self.refreshes.load(Ordering::Relaxed)
    }

    fn fresh_token_bytes(&self) -> Bytes {
        // Use the refresh counter as the unique token id so the test can
        // assert that the bytes actually changed across refresh boundaries.
        let n = self.refreshes.fetch_add(1, Ordering::Relaxed) + 1;
        Bytes::from(format!("token-#{n}"))
    }
}

impl AuthProvider for VirtualOAuthProvider {
    fn method(&self) -> &str {
        "oauth2"
    }

    fn initial(&self) -> Result<Bytes, AuthError> {
        let now = self.clock.now();
        let mut guard = self.cached.lock();
        if let Some((bytes, expires_at)) = guard.as_ref() {
            if now < *expires_at {
                return Ok(bytes.clone());
            }
        }
        let new_bytes = self.fresh_token_bytes();
        *guard = Some((new_bytes.clone(), now + self.ttl));
        Ok(new_bytes)
    }
}

#[test]
fn oauth_token_refresh_fires_exactly_at_virtual_deadline() {
    let t0 = Instant::now();
    let clock = VirtualClock::new(t0);
    let provider = VirtualOAuthProvider::new(clock.clone(), Duration::from_secs(3600));

    // First call at virtual t0 → cold cache, must fetch a fresh token.
    let token_a = provider.initial().expect("initial @ t0");
    assert_eq!(token_a.as_ref(), b"token-#1");
    assert_eq!(provider.refresh_count(), 1, "first call must refresh once");
    let calls_after_first = clock.calls();
    assert_eq!(calls_after_first, 1, "exactly one clock read on cold cache");

    // Advance to t = 3500s — well before expiry. The cached token must be
    // returned without invoking the refresh code path.
    clock.advance(Duration::from_secs(3500));
    let token_b = provider.initial().expect("initial @ t=3500s");
    assert_eq!(
        token_b.as_ref(),
        b"token-#1",
        "cached token must be reused while the virtual clock is before the deadline",
    );
    assert_eq!(
        provider.refresh_count(),
        1,
        "no refresh expected while the cached token is still fresh",
    );
    assert_eq!(
        clock.calls(),
        calls_after_first + 1,
        "the auth layer must consult the injected clock on every initial() call",
    );

    // Advance to t = 3601s — strictly past the deadline. The next call
    // must refresh.
    clock.advance(Duration::from_secs(101));
    let token_c = provider.initial().expect("initial @ t=3601s");
    assert_eq!(
        token_c.as_ref(),
        b"token-#2",
        "post-deadline call must yield a fresh token",
    );
    assert_eq!(
        provider.refresh_count(),
        2,
        "exactly one additional refresh past the deadline",
    );

    // One more call right after the refresh — cached again.
    let token_d = provider.initial().expect("initial right after refresh");
    assert_eq!(token_d.as_ref(), b"token-#2");
    assert_eq!(provider.refresh_count(), 2);
}

#[test]
fn oauth_provider_threaded_through_connection_shared() {
    // Smoke check: the auth provider plugs into `ConnectionShared::with_auth`
    // exactly the way the moonpool driver loop expects (PIP-30 / PIP-292
    // in-band token refresh path). The Clock-driven token logic above is
    // independent of the connection, but the connection wiring must
    // compile against the synthetic provider.
    let t0 = Instant::now();
    let clock = VirtualClock::new(t0);
    let provider: Arc<dyn AuthProvider> =
        Arc::new(VirtualOAuthProvider::new(clock, Duration::from_secs(3600)));
    let shared = magnetar_runtime_moonpool::ConnectionShared::with_auth(
        magnetar_proto::ConnectionConfig::default(),
        Some(provider),
    );
    // Construction succeeded — the auth provider is now reachable from
    // `ConnectionShared::auth_provider`. The driver loop's
    // `handle_pending_events` route for `ConnectionEvent::AuthChallenge`
    // consults exactly this slot.
    let _ = shared;
}
