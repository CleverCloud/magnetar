# ADR-0014 — OAuth2 `ClientCredentialsFlow` with token caching

- **Status**: Accepted
- **Date**: 2026-05-21
- **Decider**: Florentin Dubois
- **Tags**: auth, oauth2, security, sans-io

## Context

Java's `org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2` implements the OAuth2 _client_credentials_ grant against the broker's authentication plugin.
The driver:

- fetches a bearer token from a configurable `issuerUrl` / `audience`
- caches the token until shortly before its `expires_in`
- refreshes proactively (so a live `Connection` is never holding an expired bearer)

Magnetar's `magnetar-auth-oauth2` crate previously only carried a config struct.
There was no flow implementation, no token cache, no expiry-aware refresh — the parity matrix correctly listed it as `🟡`.

Two additional concerns came up in design:

- The token expiry window must be _testable_ without sleeping.
  Using `SystemTime::now()` directly would block hermetic unit tests.
- The flow must avoid the [no-channels rule (ADR-0003)](0003-no-channels-rule.md): no `tokio::sync::watch` for "current token", no `oneshot` for refresh completion.

## Decision

Implement the flow in `magnetar-auth-oauth2` as:

```rust
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> SystemTime;
}

pub struct SystemClock;
impl Clock for SystemClock { fn now(&self) -> SystemTime { SystemTime::now() } }

pub struct ClientCredentialsFlow<C: Clock = SystemClock> { /* … */ }

impl<C: Clock> ClientCredentialsFlow<C> {
    pub fn new(credentials: Credentials, clock: C) -> Self { /* … */ }
    pub async fn fetch_token(&self) -> Result<TokenResponse, OAuthError>;
}
```

Cache semantics:

- A `parking_lot::Mutex<Option<CachedToken>>` slot holds the latest token
  - the absolute expiry `SystemTime`.
- `fetch_token()` returns the cached token if `clock.now() + skew < expiry` (default skew: 30 s).
- Otherwise it does an HTTPS POST to `token_endpoint`, parses the `TokenResponse`, computes the absolute expiry, stores it, and returns.

Concurrency: no channels.
`Mutex` for the cache slot; concurrent waiters on a refresh use `tokio::sync::Notify` (planned addition) — for now, the race is benign (two parallel callers issue two POSTs, last-writer-wins).

Tests (8 cases, all using a `TestClock`):

- Empty cache → POST → cache populated.
- Cached not-yet-expired → no POST.
- Cached within skew window → POST.
- Cached expired → POST.
- POST failure surfaces `OAuthError::Http`.
- Malformed JSON surfaces `OAuthError::Decode`.
- Missing `expires_in` defaults to a sentinel TTL.
- `Credentials::from_file` reads a JSON credentials blob (Java parity).

## Consequences

- The bearer-token plumbing into `magnetar-runtime-tokio` becomes a thin wrapper: snapshot `clock.now()`, call `fetch_token().await`, hand the token to the next `AUTH_CHALLENGE`.
- The `Clock` trait is a third clock provider in the workspace (after the sans-io [ADR-0011](0011-clock-injection-sans-io.md) and moonpool's `TimeProvider`).
  It only handles `SystemTime`; `Instant` is irrelevant to OAuth2.
- Channels remain banned; the cache slot is the entire shared state.
- A future "refresh-while-still-valid background tick" can plug in without changing the cache slot's shape.

## References

- `crates/magnetar-auth-oauth2/src/lib.rs` — `ClientCredentialsFlow` + `Clock` + tests
- Commit `b22053c` — "feat(auth-oauth2): implement ClientCredentialsFlow with token caching"
- Java reference: `org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2`
- [ADR-0003 no-channels-rule](0003-no-channels-rule.md)
- [ADR-0011 clock-injection-sans-io](0011-clock-injection-sans-io.md)
