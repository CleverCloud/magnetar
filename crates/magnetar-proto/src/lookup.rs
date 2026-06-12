// SPDX-License-Identifier: Apache-2.0

//! Binary-protocol topic lookup state machine.
//!
//! Port of `org.apache.pulsar.client.impl.BinaryProtoLookupService`. We model the lookup as a
//! tiny per-request state machine that handles redirects internally — the user-visible
//! outcome is either [`LookupOutcome::Connect`] or [`LookupOutcome::Failed`].
//!
//! [`LookupOutcome::Redirected`] is **diagnostic only**: it is surfaced via
//! [`crate::event::ConnectionEvent::LookupResponse`] for tracing / observability
//! but is **never** delivered to the user-facing waker (HIGH-4 fix from the
//! lookup multi-agent review). The state machine chases each `Redirect` to its
//! terminal outcome internally, then publishes that terminal outcome on the
//! *original* user-facing request-id (`chain_origin`) so the engine future
//! only wakes once with the final answer. This is what makes
//! [`MAX_LOOKUP_REDIRECTS`] (the redirect cap) and the broker-URL passthrough
//! end-to-end user-observable instead of being folded into a no-op by the
//! engine's "first-hop wins" handling.
//!
//! # References
//!
//! - `BinaryProtoLookupService.java:56` (entry point)
//! - `BinaryProtoLookupService.java:146` (redirect handling)
//! - `BinaryProtoLookupService.java:260` (partitioned-topic metadata)

use std::collections::{HashMap, HashSet};

use crate::event::LookupOutcome;
use crate::pb;
use crate::types::RequestId;

/// Maximum number of redirect hops the lookup state machine will chase before
/// failing with `LookupOutcome::Failed { code: 0, message: "lookup redirect
/// cap exceeded …" }`.
///
/// Mirrors Java `BinaryProtoLookupService.MaxLookupRedirects = 5`. A
/// misbehaving or hostile broker that keeps returning `Redirect` cannot make
/// the client allocate request-ids / registry entries forever — closes the
/// "redirect-loop DoS" finding from the lookup multi-agent review.
pub const MAX_LOOKUP_REDIRECTS: u8 = 5;

/// Maximum number of times a runtime engine re-issues a `CommandLookupTopic`
/// after the in-flight request was severed by a supervised reconnect — i.e.
/// after [`crate::Connection::reset`] published an
/// [`crate::OpOutcome::SessionLost`] on the lookup's request-id.
///
/// [`crate::Connection::reset`] deliberately fails every pending request
/// (including an in-flight `CommandLookupTopic`) with
/// [`crate::OpOutcome::SessionLost`] so a supervised reconnect can rebuild the
/// session cleanly — but, unlike an in-flight publish (which `reset` snapshots
/// for transparent replay), a lookup is *not* re-issued by the proto layer.
/// The engine-side lookup path closes that asymmetry: on `SessionLost` it parks
/// until the connection is live again (or terminal), then re-issues the lookup
/// against the fresh session. This const bounds how many such re-issues a
/// single `lookup_topic` call will attempt before giving up, so a connection
/// that keeps flapping right as the lookup lands cannot spin forever.
///
/// A re-issue counts against this budget **only** when a lookup was actually
/// submitted against a connected session — spurious driver wakes / repeated
/// `SessionLost` within the same not-yet-reconnected window do not burn the
/// budget. Mirrors the lookup-after-reset retry in Java's
/// `BinaryProtoLookupService` (each reconnect re-drives the pending lookup
/// future). Lives next to [`MAX_LOOKUP_REDIRECTS`] so the two lookup caps share
/// a single source of truth; a plain `const` adds no I/O dependency, keeping
/// `magnetar-proto` zero-I/O (ADR-0004). See ADR-0060.
pub const MAX_LOOKUP_SESSION_REISSUES: u8 = 5;

/// Return the zero-based partition index iff `topic` is a per-partition
/// child of a partitioned topic — i.e. its tail matches `-partition-<N>`
/// where `<N>` is a non-negative decimal integer and the segment before
/// `-partition-` is non-empty.
///
/// Mirrors Java `org.apache.pulsar.common.naming.TopicName#isPartitioned` /
/// `getPartitionIndex`, the canonical detector the broker uses to decide
/// whether the name already encodes a partition. The match is anchored at
/// end-of-string: `my-partition-foo-3` is a partition, `my-partition-foo`
/// is not, `my-partition-` is not.
///
/// Used by [`crate::Connection::get_partitioned_topic_metadata`] to
/// short-circuit the broker round-trip when the input is already a
/// per-partition child name — for a 12-partition topic this cuts 13
/// `CommandPartitionedTopicMetadata` frames to 1, reducing both broker
/// load and downstream metadata-store amplification. Mirrors the
/// equivalent fast-path in streamnative-pulsar-rs #327.
///
/// # Detection rule
///
/// The implementation matches Java's exact behaviour: we rsplit at the
/// last `-`, require the suffix to be a non-empty all-ASCII-digits
/// substring that parses as [`u32`], and require the preceding text to
/// end with `-partition` (i.e. the topic name contains
/// `-partition-<digits>` strictly at the tail). This is more conservative
/// than the streamnative patch's `contains("-partition-")` which
/// false-positives on names like `my-partition-thing-3` (where `thing-3`
/// is the trailing segment, not a partition index).
///
/// Returns `None` for non-partitioned names, names ending in
/// `-partition-` with no digits, names ending in non-numeric tails,
/// names whose partition index overflows `u32`, and the empty string.
///
/// Public so runtime callers can short-circuit earlier in their stack
/// (e.g. multi-topic builders that resolve a parent topic to its
/// per-partition children).
#[must_use]
pub fn topic_partition_index(topic: &str) -> Option<u32> {
    let (prefix, suffix) = topic.rsplit_once('-')?;
    if suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !prefix.ends_with("-partition") {
        return None;
    }
    // Reject the degenerate case where the prefix is exactly "-partition"
    // (i.e. the topic is literally "-partition-N" with no namespace
    // segment) — Java's `TopicName` parser rejects empty topic names so
    // we mirror that here.
    if prefix.len() == "-partition".len() {
        return None;
    }
    suffix.parse::<u32>().ok()
}

/// Convenience wrapper around [`topic_partition_index`] — returns `true`
/// iff the topic name already encodes a partition index per Java's
/// `TopicName#isPartitioned`.
#[must_use]
pub fn is_partition_topic(topic: &str) -> bool {
    topic_partition_index(topic).is_some()
}

/// In-flight state for a single lookup request.
#[derive(Debug, Clone)]
pub(crate) struct LookupRequest {
    /// The topic being looked up.
    pub(crate) topic: String,
    /// Whether the next round-trip should be authoritative.
    pub(crate) authoritative: bool,
    /// Remaining redirect hops before the chain fails. Initialised to
    /// [`MAX_LOOKUP_REDIRECTS`] by the public entry point; decremented by
    /// [`translate_lookup_response`] on each `Redirect` retry. When `0`, the
    /// next `Redirect` short-circuits to `Failed` instead of issuing another
    /// hop.
    pub(crate) hops_remaining: u8,
    /// The *user-facing* request id this lookup chain is anchored on.
    ///
    /// The wire-level request id changes on every redirect hop (each hop
    /// allocates a fresh id so the broker can correlate its own state) but
    /// the [`crate::event::OpOutcome::LookupResponse`] surfaced to the user
    /// is always keyed on `chain_origin` — the request id returned by the
    /// initial [`crate::Connection::lookup`] call. HIGH-4 (lookup
    /// multi-agent review): only terminal outcomes (`Connect` / `Failed`)
    /// are delivered against `chain_origin`; intermediate `Redirected`
    /// outcomes are pushed to the events queue for diagnostics only and
    /// never wake the user-facing future. This is what makes the redirect
    /// cap and the broker-URL passthrough end-to-end user-observable.
    pub(crate) chain_origin: RequestId,
}

/// Reason a lookup-registry insert failed without ever touching the wire.
///
/// Surfaced by [`LookupRegistry::insert_lookup`] /
/// [`LookupRegistry::insert_partition`] when the configured
/// `max_pending_lookups` cap (see
/// [`crate::conn::ConnectionConfig::max_pending_lookups`]) is exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LookupRejected {
    /// The connection-wide cap on in-flight lookup + partitioned-metadata
    /// requests was already reached. The caller must surface this to the user
    /// (typically as a synthetic [`LookupOutcome::Failed`]) rather than
    /// allocating a request-id slot.
    MaxPending,
}

/// Why an outbound lookup submission failed before the broker ever saw it.
///
/// Sits between [`LookupRejected`] (cap exhausted) and the historic
/// silently-ignored encode failure mode. Engines do not see this type — the
/// conn layer turns it into a synthetic [`LookupOutcome::Failed`] surfaced
/// via the waker slab when the cap kicks in. Encode failures still drop
/// silently, matching the pre-hardening behaviour (a follow-up could
/// promote them to a synthetic Failed; tracked separately).
#[derive(Debug)]
pub(crate) enum LookupSubmitError {
    /// The registry rejected the insert because the connection's
    /// `max_pending_lookups` cap was already reached.
    Rejected,
    /// `encode_command` rejected the frame (oversize, etc.). The registry
    /// slot was already reserved; the conn layer treats this the same as
    /// `Ok(())` (the lookup waits in the registry until timeout / reset),
    /// matching the pre-hardening silent-drop behaviour.
    Encode,
}

/// Container for in-flight lookup state.
///
/// Owned by [`Connection`](crate::Connection); used to decide whether an incoming
/// `CommandLookupTopicResponse` belongs to us and what to do next.
#[derive(Debug, Default)]
pub(crate) struct LookupRegistry {
    pub(crate) lookups: HashMap<RequestId, LookupRequest>,
    /// In-flight `CommandPartitionedTopicMetadata` request ids. The response
    /// path only needs to know whether `request_id` was issued by us
    /// ([`Connection::take_partition`] returns `bool`); the queried topic
    /// itself is echoed by the broker on the response frame, so no per-entry
    /// payload is kept. Tracking the bare id (rather than a `(rid, topic)`
    /// pair) drops one `String` clone per partitioned-metadata lookup.
    pub(crate) partitions: HashSet<RequestId>,
    /// Cap on the *total* number of in-flight lookup + partitioned-metadata
    /// requests. `0` (the default) means unbounded, matching the historical
    /// Java behaviour. Set via
    /// [`crate::conn::ConnectionConfig::max_pending_lookups`].
    pub(crate) max_pending: usize,
}

impl LookupRegistry {
    /// Total number of in-flight entries (lookup + partitioned-metadata).
    /// The cap is enforced on this sum so a hostile broker cannot bypass it
    /// by mixing the two request types.
    pub(crate) fn total_pending(&self) -> usize {
        self.lookups.len() + self.partitions.len()
    }

    fn has_capacity(&self) -> bool {
        self.max_pending == 0 || self.total_pending() < self.max_pending
    }

    pub(crate) fn insert_lookup(
        &mut self,
        request_id: RequestId,
        req: LookupRequest,
    ) -> Result<(), LookupRejected> {
        if !self.has_capacity() {
            return Err(LookupRejected::MaxPending);
        }
        self.lookups.insert(request_id, req);
        Ok(())
    }

    pub(crate) fn insert_partition(&mut self, request_id: RequestId) -> Result<(), LookupRejected> {
        if !self.has_capacity() {
            return Err(LookupRejected::MaxPending);
        }
        self.partitions.insert(request_id);
        Ok(())
    }

    pub(crate) fn take_lookup(&mut self, request_id: RequestId) -> Option<LookupRequest> {
        self.lookups.remove(&request_id)
    }

    /// Returns `true` iff `request_id` was an in-flight partitioned-metadata
    /// request and has now been claimed. One-shot: a second call with the
    /// same id returns `false`.
    pub(crate) fn take_partition(&mut self, request_id: RequestId) -> bool {
        self.partitions.remove(&request_id)
    }

    /// Snapshot every in-flight lookup + partitioned-metadata request id.
    ///
    /// Used by [`crate::conn::Connection::reset`] to publish
    /// `OpOutcome::SessionLost` against every parked `RequestFut`
    /// (runtime-side) **before** the registry is cleared. Closes the lookup multi-agent review
    /// HIGH-3 race window: without this drain, a lookup whose request id never made it into
    /// `pending_requests` (defensive path) — or a future that lost its
    /// race to register a waker before the reset fired — would park on its
    /// waker until the runtime's `operation_timeout` (default 30s) instead
    /// of resolving immediately. Belt-and-suspenders relative to the
    /// `pending_requests` drain in `reset`: every legitimate in-flight
    /// lookup is already keyed in both maps today, but a future refactor
    /// that desynchronises them would silently re-introduce the race
    /// without this guard.
    ///
    /// The returned vector is the union of `lookups.keys()` and
    /// `partitions.iter()`. Duplicates are impossible — both collections
    /// allocate from the same request-id space and the cap path
    /// (`insert_lookup` / `insert_partition`) keys on the same `RequestId`
    /// type — so the caller's outcome-publish loop is idempotent regardless.
    pub(crate) fn pending_request_ids(&self) -> Vec<RequestId> {
        let mut out = Vec::with_capacity(self.total_pending());
        out.extend(self.lookups.keys().copied());
        out.extend(self.partitions.iter().copied());
        out
    }
}

/// Translate a `CommandLookupTopicResponse` into the user-facing outcome and an optional
/// "retry lookup" decision.
///
/// Returns `(outcome, Some(LookupRequest))` if the broker asked us to retry with authoritative
/// (Redirect). The caller is responsible for emitting the retry frame.
pub(crate) fn translate_lookup_response(
    response: &pb::CommandLookupTopicResponse,
    request: &LookupRequest,
) -> (LookupOutcome, Option<LookupRequest>) {
    use pb::command_lookup_topic_response::LookupType;

    let lookup_type = response
        .response
        .and_then(|v| LookupType::try_from(v).ok())
        .unwrap_or(LookupType::Failed);

    match lookup_type {
        LookupType::Connect => (
            LookupOutcome::Connect {
                broker_service_url: response.broker_service_url.clone(),
                broker_service_url_tls: response.broker_service_url_tls.clone(),
                proxy_through_service_url: response.proxy_through_service_url.unwrap_or(false),
            },
            None,
        ),
        LookupType::Redirect => {
            // Hardening pass (lookup multi-agent review): short-circuit to
            // `Failed` once the chain has burnt through `MAX_LOOKUP_REDIRECTS`
            // hops. Stops a hostile broker from exhausting request-ids and
            // registry entries via an unbounded redirect chain.
            if request.hops_remaining == 0 {
                return (
                    LookupOutcome::Failed {
                        code: 0,
                        message: format!(
                            "lookup redirect cap exceeded ({MAX_LOOKUP_REDIRECTS} hops)"
                        ),
                    },
                    None,
                );
            }
            let retry = LookupRequest {
                topic: request.topic.clone(),
                // Per Java: after a redirect, the next round-trip is marked authoritative
                // iff the broker said so. Default to `true` to bound the recursion depth.
                authoritative: response.authoritative.unwrap_or(true),
                hops_remaining: request.hops_remaining - 1,
                // Carry the user-facing anchor through every hop of the
                // chain — the terminal outcome is delivered against this
                // id, not the per-hop wire id.
                chain_origin: request.chain_origin,
            };
            (
                LookupOutcome::Redirected {
                    broker_service_url: response.broker_service_url.clone(),
                    broker_service_url_tls: response.broker_service_url_tls.clone(),
                },
                Some(retry),
            )
        }
        LookupType::Failed => (
            LookupOutcome::Failed {
                code: response.error.unwrap_or(0),
                message: response.message.clone().unwrap_or_default(),
            },
            None,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper: build a `LookupRequest` seeded with the full
    /// `MAX_LOOKUP_REDIRECTS` hop budget. Mirrors what `Connection::lookup`
    /// constructs on the public entry path. The `chain_origin` defaults to
    /// `RequestId(1)` — tests that care about the anchor override it
    /// explicitly.
    fn fresh_lookup_request(topic: &str, authoritative: bool) -> LookupRequest {
        LookupRequest {
            topic: topic.to_owned(),
            authoritative,
            hops_remaining: MAX_LOOKUP_REDIRECTS,
            chain_origin: RequestId(1),
        }
    }

    #[test]
    fn connect_outcome_does_not_retry() {
        let resp = pb::CommandLookupTopicResponse {
            broker_service_url: Some("pulsar://broker:6650".to_owned()),
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
            request_id: 1,
            authoritative: Some(false),
            error: None,
            message: None,
            proxy_through_service_url: Some(false),
        };
        let req = fresh_lookup_request("persistent://public/default/foo", false);
        let (out, retry) = translate_lookup_response(&resp, &req);
        assert!(retry.is_none());
        matches!(out, LookupOutcome::Connect { .. });
    }

    /// ADR-0060 / follow-ups §4.1: the engine-side lookup-retry-on-`SessionLost`
    /// loop is bounded by [`MAX_LOOKUP_SESSION_REISSUES`]. The const must be
    /// non-zero (at least one re-issue is allowed — otherwise a single transient
    /// reconnect would surface `PeerClosed` and defeat the whole point) and
    /// small enough that a persistently flapping connection cannot spin a
    /// `lookup_topic` call for long before it gives up. This pins the
    /// single-source-of-truth bound the two engines share so neither can
    /// silently re-introduce an unbounded loop.
    #[test]
    fn max_lookup_session_reissues_is_bounded_and_nonzero() {
        // Compile-time const-block assertions: the bound is a `const`, so these
        // are enforced at build time, not just at test time.
        const {
            assert!(
                MAX_LOOKUP_SESSION_REISSUES >= 1,
                "at least one re-issue must be allowed so a single transient \
                 SessionLost recovers transparently",
            );
        }
        const {
            assert!(
                MAX_LOOKUP_SESSION_REISSUES <= 16,
                "the reissue budget must stay small so a flapping connection cannot \
                 spin a lookup_topic call for long",
            );
        }
    }

    #[test]
    fn redirect_outcome_requests_authoritative_retry() {
        let resp = pb::CommandLookupTopicResponse {
            broker_service_url: Some("pulsar://other:6650".to_owned()),
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
            request_id: 1,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: Some(false),
        };
        let req = fresh_lookup_request("persistent://public/default/foo", false);
        let (out, retry) = translate_lookup_response(&resp, &req);
        matches!(out, LookupOutcome::Redirected { .. });
        let retry = retry.expect("redirect should produce a retry");
        assert!(retry.authoritative);
        // The fresh request's hop budget decrements by exactly one per
        // redirect — ensures the cap is enforced via the retry's carried
        // counter rather than implicit recursion depth.
        assert_eq!(retry.hops_remaining, MAX_LOOKUP_REDIRECTS - 1);
    }

    #[test]
    fn failed_outcome_propagates_error() {
        let resp = pb::CommandLookupTopicResponse {
            broker_service_url: None,
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Failed as i32),
            request_id: 1,
            authoritative: Some(false),
            error: Some(pb::ServerError::ServiceNotReady as i32),
            message: Some("svc not ready".to_owned()),
            proxy_through_service_url: Some(false),
        };
        let req = fresh_lookup_request("persistent://public/default/foo", false);
        let (out, retry) = translate_lookup_response(&resp, &req);
        assert!(retry.is_none());
        match out {
            LookupOutcome::Failed { message, .. } => assert_eq!(message, "svc not ready"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// Ported from Java `BinaryProtoLookupService` — a `Redirect` response whose
    /// `authoritative` field is absent (older brokers, or a bug in the server path) must
    /// still default the retry to `authoritative = true`. Without that, the redirect loop
    /// would never terminate because the broker might keep redirecting in a chain.
    #[test]
    fn redirect_missing_authoritative_defaults_to_true() {
        let resp = pb::CommandLookupTopicResponse {
            broker_service_url: Some("pulsar://other:6650".to_owned()),
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
            request_id: 1,
            authoritative: None, // broker omitted the field
            error: None,
            message: None,
            proxy_through_service_url: None,
        };
        let req = fresh_lookup_request("persistent://public/default/foo", false);
        let (_out, retry) = translate_lookup_response(&resp, &req);
        let retry = retry.expect("redirect should produce a retry");
        assert!(
            retry.authoritative,
            "missing authoritative field must default to true to bound the chain"
        );
        // Topic is carried verbatim from the original request.
        assert_eq!(retry.topic, "persistent://public/default/foo");
    }

    /// Ported from Java `BinaryProtoLookupService#getBroker`. The `Connect` outcome must
    /// honour `proxy_through_service_url` so the runtime can decide whether to dial the
    /// proxy or the broker directly (PIP-37). A missing field falls back to `false`.
    #[test]
    fn connect_outcome_honours_proxy_through_service_url() {
        let resp = pb::CommandLookupTopicResponse {
            broker_service_url: Some("pulsar://broker:6650".to_owned()),
            broker_service_url_tls: Some("pulsar+ssl://broker:6651".to_owned()),
            response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
            request_id: 1,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: Some(true),
        };
        let req = fresh_lookup_request("persistent://public/default/foo", true);
        let (out, retry) = translate_lookup_response(&resp, &req);
        assert!(retry.is_none(), "connect never retries");
        match out {
            LookupOutcome::Connect {
                broker_service_url,
                broker_service_url_tls,
                proxy_through_service_url,
            } => {
                assert_eq!(broker_service_url.as_deref(), Some("pulsar://broker:6650"));
                assert_eq!(
                    broker_service_url_tls.as_deref(),
                    Some("pulsar+ssl://broker:6651")
                );
                assert!(proxy_through_service_url);
            }
            other => panic!("expected Connect, got {other:?}"),
        }
    }

    /// Ported from Java `BinaryProtoLookupService` — a response whose `response` field is
    /// missing (malformed broker reply) must fall through to `Failed` rather than panic.
    /// This protects the driver against future protocol drift.
    #[test]
    fn missing_response_field_falls_through_to_failed() {
        let resp = pb::CommandLookupTopicResponse {
            broker_service_url: None,
            broker_service_url_tls: None,
            response: None, // malformed
            request_id: 1,
            authoritative: None,
            error: None,
            message: None,
            proxy_through_service_url: None,
        };
        let req = fresh_lookup_request("persistent://public/default/foo", false);
        let (out, retry) = translate_lookup_response(&resp, &req);
        assert!(retry.is_none());
        assert!(matches!(out, LookupOutcome::Failed { .. }));
    }

    /// Ported from Java `BinaryProtoLookupService#getPartitionedTopicMetadata` — a failed
    /// lookup with no broker-side `message` field defaults to an empty string. Callers must
    /// be able to `match` on the `Failed` variant without optionalising the message.
    #[test]
    fn failed_outcome_missing_message_falls_back_to_empty_string() {
        let resp = pb::CommandLookupTopicResponse {
            broker_service_url: None,
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Failed as i32),
            request_id: 1,
            authoritative: Some(false),
            error: Some(pb::ServerError::AuthorizationError as i32),
            message: None, // broker forgot the message
            proxy_through_service_url: None,
        };
        let req = fresh_lookup_request("persistent://public/default/foo", false);
        let (out, _) = translate_lookup_response(&resp, &req);
        match out {
            LookupOutcome::Failed { code, message } => {
                assert_eq!(code, pb::ServerError::AuthorizationError as i32);
                assert_eq!(message, "");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// `LookupRegistry` is a one-shot map keyed by `RequestId`. The runtime relies on
    /// `take_lookup` consuming the entry so a duplicate broker response (e.g. a stale frame
    /// arriving after the supervisor reset) cannot accidentally trigger a second retry. Pin
    /// the behavior so refactors do not change it silently.
    #[test]
    fn lookup_registry_take_is_one_shot() {
        let mut reg = LookupRegistry::default();
        let rid = RequestId(7);
        reg.insert_lookup(
            rid,
            fresh_lookup_request("persistent://public/default/foo", true),
        )
        .expect("default LookupRegistry has unbounded capacity");
        assert!(reg.take_lookup(rid).is_some(), "first take returns Some");
        assert!(
            reg.take_lookup(rid).is_none(),
            "duplicate take returns None — guards against double-handling"
        );
    }

    /// `LookupRegistry::take_partition` must mirror `take_lookup`'s one-shot semantics. The
    /// runtime keys partitioned-metadata responses by `request_id`; a duplicate response
    /// must be silently dropped.
    #[test]
    fn lookup_registry_take_partition_is_one_shot() {
        let mut reg = LookupRegistry::default();
        let rid = RequestId(99);
        reg.insert_partition(rid)
            .expect("default LookupRegistry has unbounded capacity");
        assert!(reg.take_partition(rid), "first take claims the entry");
        assert!(
            !reg.take_partition(rid),
            "duplicate take returns false — no second claim"
        );
    }

    /// HIGH-2 (lookup multi-agent review): once the chain has burnt through
    /// `MAX_LOOKUP_REDIRECTS` hops, the next `Redirect` short-circuits to
    /// `Failed` rather than producing yet another retry request. This is
    /// what stops a hostile broker from driving an unbounded redirect loop
    /// through the client.
    #[test]
    fn redirect_chain_terminates_at_cap() {
        let resp = pb::CommandLookupTopicResponse {
            broker_service_url: Some("pulsar://other:6650".to_owned()),
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
            request_id: 1,
            authoritative: Some(false),
            error: None,
            message: None,
            proxy_through_service_url: None,
        };
        // Exhausted hop budget — the next redirect must surface as Failed.
        let req = LookupRequest {
            topic: "persistent://public/default/foo".to_owned(),
            authoritative: false,
            hops_remaining: 0,
            chain_origin: RequestId(1),
        };
        let (out, retry) = translate_lookup_response(&resp, &req);
        assert!(retry.is_none(), "no retry once cap is hit");
        match out {
            LookupOutcome::Failed { code, message } => {
                assert_eq!(code, 0);
                assert!(
                    message.contains("redirect cap exceeded"),
                    "diagnostic message must mention the cap, got: {message}"
                );
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// HIGH-4 (lookup multi-agent review): every retry produced by
    /// `translate_lookup_response` must carry the *original* user-facing
    /// `chain_origin` unchanged. The wire-level `request_id` in the
    /// outbound `CommandLookupTopic` will be allocated fresh by the caller
    /// (`Connection::send_lookup_internal`), but the user's future only
    /// ever wakes against `chain_origin` — so losing the anchor between
    /// hops would either silently drop the terminal outcome or wake the
    /// wrong future.
    #[test]
    fn redirect_retry_carries_chain_origin_unchanged() {
        let resp = pb::CommandLookupTopicResponse {
            broker_service_url: Some("pulsar://other:6650".to_owned()),
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
            request_id: 42, // wire-level id — orthogonal to chain_origin
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: None,
        };
        // Anchor on a deliberately non-trivial id so a "copy the wire id"
        // bug surfaces as a mismatch instead of a coincidental match.
        let origin = RequestId(7);
        let req = LookupRequest {
            topic: "persistent://public/default/foo".to_owned(),
            authoritative: false,
            hops_remaining: MAX_LOOKUP_REDIRECTS,
            chain_origin: origin,
        };
        let (_out, retry) = translate_lookup_response(&resp, &req);
        let retry = retry.expect("redirect should produce a retry");
        assert_eq!(
            retry.chain_origin, origin,
            "chain_origin must be preserved verbatim across every redirect hop"
        );
    }

    /// HIGH-2 follow-up: walk the redirect chain N steps and confirm the
    /// hop counter monotonically decrements without ever wrapping around.
    /// Without this guard, a `u8` underflow would silently re-enable an
    /// unbounded loop.
    #[test]
    fn redirect_hops_monotonically_decrement() {
        let mut req = fresh_lookup_request("persistent://public/default/foo", false);
        let mk_redirect = || pb::CommandLookupTopicResponse {
            broker_service_url: Some("pulsar://other:6650".to_owned()),
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
            request_id: 1,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: None,
        };
        for expected_remaining in (0..MAX_LOOKUP_REDIRECTS).rev() {
            let (_out, retry) = translate_lookup_response(&mk_redirect(), &req);
            let retry = retry.expect("hops > 0 must produce a retry");
            assert_eq!(retry.hops_remaining, expected_remaining);
            req = retry;
        }
        // One more redirect on a zeroed budget must short-circuit to Failed.
        let (out, retry) = translate_lookup_response(&mk_redirect(), &req);
        assert!(retry.is_none());
        assert!(matches!(out, LookupOutcome::Failed { .. }));
    }

    /// MEDIUM-2 (lookup multi-agent review): `max_pending = 0` preserves
    /// the historical unbounded behaviour (Java parity).
    #[test]
    fn lookup_registry_max_pending_zero_is_unbounded() {
        let mut reg = LookupRegistry {
            max_pending: 0,
            ..Default::default()
        };
        for i in 0..256u64 {
            reg.insert_lookup(RequestId(i), fresh_lookup_request("t", false))
                .expect("zero cap means unbounded");
        }
        assert_eq!(reg.total_pending(), 256);
    }

    /// MEDIUM-2: when `max_pending` is set, the (N+1)-th insert is rejected
    /// with `LookupRejected::MaxPending` and the registry stays at the cap.
    /// Closes the "pending-lookup memory amplification" finding.
    #[test]
    fn lookup_registry_max_pending_rejects_at_cap() {
        let mut reg = LookupRegistry {
            max_pending: 3,
            ..Default::default()
        };
        for i in 0..3u64 {
            reg.insert_lookup(RequestId(i), fresh_lookup_request("t", false))
                .expect("under cap");
        }
        let rejected = reg.insert_lookup(RequestId(99), fresh_lookup_request("t", false));
        assert_eq!(rejected, Err(LookupRejected::MaxPending));
        assert_eq!(
            reg.total_pending(),
            3,
            "rejected insert must not advance the count"
        );
    }

    /// MEDIUM-2: the cap is applied to the *sum* of lookup +
    /// partitioned-metadata entries — a hostile broker cannot bypass the
    /// cap by mixing the two request types.
    #[test]
    fn lookup_registry_max_pending_is_combined_total() {
        let mut reg = LookupRegistry {
            max_pending: 2,
            ..Default::default()
        };
        reg.insert_lookup(RequestId(1), fresh_lookup_request("a", false))
            .expect("under cap");
        reg.insert_partition(RequestId(2)).expect("still under cap");
        // Third insert (either kind) must be rejected.
        let rejected_lookup = reg.insert_lookup(RequestId(3), fresh_lookup_request("c", false));
        assert_eq!(rejected_lookup, Err(LookupRejected::MaxPending));
        let rejected_partition = reg.insert_partition(RequestId(4));
        assert_eq!(rejected_partition, Err(LookupRejected::MaxPending));
    }

    /// Java parity for `TopicName#isPartitioned` — names that already
    /// encode a partition index must be detected so the
    /// `get_partitioned_topic_metadata` fast-path can short-circuit them.
    /// The accepted shape is `<base>-partition-<digits>` strictly at
    /// end-of-string, with a non-empty `<base>`.
    #[test]
    fn topic_partition_index_detects_well_formed_suffixes() {
        // Canonical Pulsar partition-topic shape.
        assert_eq!(
            topic_partition_index("persistent://public/default/foo-partition-0"),
            Some(0),
            "zero partition index is legal"
        );
        assert_eq!(
            topic_partition_index("persistent://public/default/foo-partition-12"),
            Some(12),
        );
        assert_eq!(
            topic_partition_index("persistent://public/default/foo-partition-9999"),
            Some(9999),
        );
        // Java's TopicName accepts (and emits) leading zeroes on the index;
        // they parse fine through `u32::from_str`. Mirror that.
        assert_eq!(
            topic_partition_index("persistent://public/default/foo-partition-007"),
            Some(7),
        );
        // No namespace path — bare base + partition.
        assert_eq!(topic_partition_index("foo-partition-3"), Some(3));
        // The convenience wrapper agrees.
        assert!(is_partition_topic(
            "persistent://public/default/foo-partition-0"
        ));
    }

    /// The detection rule must NOT match topics whose trailing segment is
    /// not a numeric partition index. This is the false-positive trap the
    /// streamnative `contains("-partition-")` heuristic falls into; we
    /// reject it explicitly so the fast-path stays Java-correct.
    #[test]
    fn topic_partition_index_rejects_false_positives() {
        // The literal trap from the F11 spec: a topic named
        // `my-partition-thing-3` contains the substring `-partition-` but
        // its trailing segment is `3` on `my-partition-thing`, not a
        // partition index. The Java rule (and ours) rejects it because
        // `thing` is not `-partition`.
        assert_eq!(topic_partition_index("my-partition-thing-3"), None);
        // Tail is non-numeric.
        assert_eq!(topic_partition_index("foo-partition-foo"), None);
        // Trailing dash with no digits.
        assert_eq!(topic_partition_index("foo-partition-"), None);
        // No `-partition-` suffix at all.
        assert_eq!(topic_partition_index("foo"), None);
        // Empty input.
        assert_eq!(topic_partition_index(""), None);
        // `-partition-` somewhere in the middle, but the tail is a
        // different segment (no numeric suffix).
        assert_eq!(topic_partition_index("foo-partition-3-bar"), None);
        // Bare `-partition-N` with no base — Java rejects empty topic
        // names; we mirror that.
        assert_eq!(topic_partition_index("-partition-0"), None);
        // The convenience wrapper agrees.
        assert!(!is_partition_topic("my-partition-thing-3"));
        assert!(!is_partition_topic("foo-partition-"));
    }

    /// A partition index that overflows `u32` must NOT match — the broker
    /// would reject it anyway and silently truncating it would be worse
    /// than just failing the fast-path and letting the LOOKUP round-trip
    /// surface the broker's diagnostic.
    #[test]
    fn topic_partition_index_rejects_u32_overflow() {
        // u32::MAX is 4_294_967_295; add one to overflow.
        assert_eq!(topic_partition_index("foo-partition-4294967296"), None);
        // Plus-sign / minus-sign rejected (not ASCII digits).
        assert_eq!(topic_partition_index("foo-partition-+3"), None);
        assert_eq!(topic_partition_index("foo-partition--3"), None);
    }

    /// MEDIUM-2: capacity opens back up after a `take`, so a steady-state
    /// workload at the cap can still make forward progress as broker
    /// responses arrive.
    #[test]
    fn lookup_registry_max_pending_reopens_after_take() {
        let mut reg = LookupRegistry {
            max_pending: 1,
            ..Default::default()
        };
        let r1 = RequestId(1);
        let r2 = RequestId(2);
        reg.insert_lookup(r1, fresh_lookup_request("a", false))
            .expect("first under cap");
        assert_eq!(
            reg.insert_lookup(r2, fresh_lookup_request("b", false)),
            Err(LookupRejected::MaxPending),
        );
        // Drain r1 — capacity opens up for r2.
        assert!(reg.take_lookup(r1).is_some());
        reg.insert_lookup(r2, fresh_lookup_request("b", false))
            .expect("capacity reopened after take");
    }
}
