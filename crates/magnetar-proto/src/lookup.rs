// SPDX-License-Identifier: Apache-2.0

//! Binary-protocol topic lookup state machine.
//!
//! Port of `org.apache.pulsar.client.impl.BinaryProtoLookupService`. We model the lookup as a
//! tiny per-request state machine that handles redirects internally — the user-visible event
//! is either `LookupResponse::Connect`, `LookupResponse::Redirected` (for diagnostics), or
//! `LookupResponse::Failed`.
//!
//! # References
//!
//! - `BinaryProtoLookupService.java:56` (entry point)
//! - `BinaryProtoLookupService.java:146` (redirect handling)
//! - `BinaryProtoLookupService.java:260` (partitioned-topic metadata)

use std::collections::HashMap;

use crate::event::LookupOutcome;
use crate::pb;
use crate::types::RequestId;

/// In-flight state for a single lookup request.
#[derive(Debug, Clone)]
pub(crate) struct LookupRequest {
    /// The topic being looked up.
    pub(crate) topic: String,
    /// Whether the next round-trip should be authoritative.
    pub(crate) authoritative: bool,
}

/// In-flight state for a single partitioned-topic metadata request.
#[derive(Debug, Clone)]
pub(crate) struct PartitionedMetadataRequest {
    /// The topic being queried.
    pub(crate) topic: String,
}

/// Container for in-flight lookup state.
///
/// Owned by [`Connection`](crate::Connection); used to decide whether an incoming
/// `CommandLookupTopicResponse` belongs to us and what to do next.
#[derive(Debug, Default)]
pub(crate) struct LookupRegistry {
    pub(crate) lookups: HashMap<RequestId, LookupRequest>,
    pub(crate) partitions: HashMap<RequestId, PartitionedMetadataRequest>,
}

impl LookupRegistry {
    pub(crate) fn insert_lookup(&mut self, request_id: RequestId, req: LookupRequest) {
        self.lookups.insert(request_id, req);
    }

    pub(crate) fn insert_partition(
        &mut self,
        request_id: RequestId,
        req: PartitionedMetadataRequest,
    ) {
        self.partitions.insert(request_id, req);
    }

    pub(crate) fn take_lookup(&mut self, request_id: RequestId) -> Option<LookupRequest> {
        self.lookups.remove(&request_id)
    }

    pub(crate) fn take_partition(
        &mut self,
        request_id: RequestId,
    ) -> Option<PartitionedMetadataRequest> {
        self.partitions.remove(&request_id)
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
            let retry = LookupRequest {
                topic: request.topic.clone(),
                // Per Java: after a redirect, the next round-trip is marked authoritative
                // iff the broker said so. Default to `true` to bound the recursion depth.
                authoritative: response.authoritative.unwrap_or(true),
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
        let req = LookupRequest {
            topic: "persistent://public/default/foo".to_owned(),
            authoritative: false,
        };
        let (out, retry) = translate_lookup_response(&resp, &req);
        assert!(retry.is_none());
        matches!(out, LookupOutcome::Connect { .. });
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
        let req = LookupRequest {
            topic: "persistent://public/default/foo".to_owned(),
            authoritative: false,
        };
        let (out, retry) = translate_lookup_response(&resp, &req);
        matches!(out, LookupOutcome::Redirected { .. });
        let retry = retry.expect("redirect should produce a retry");
        assert!(retry.authoritative);
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
        let req = LookupRequest {
            topic: "persistent://public/default/foo".to_owned(),
            authoritative: false,
        };
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
        let req = LookupRequest {
            topic: "persistent://public/default/foo".to_owned(),
            authoritative: false,
        };
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
        let req = LookupRequest {
            topic: "persistent://public/default/foo".to_owned(),
            authoritative: true,
        };
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
        let req = LookupRequest {
            topic: "persistent://public/default/foo".to_owned(),
            authoritative: false,
        };
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
        let req = LookupRequest {
            topic: "persistent://public/default/foo".to_owned(),
            authoritative: false,
        };
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
            LookupRequest {
                topic: "persistent://public/default/foo".to_owned(),
                authoritative: true,
            },
        );
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
        reg.insert_partition(
            rid,
            PartitionedMetadataRequest {
                topic: "persistent://public/default/p".to_owned(),
            },
        );
        let taken = reg.take_partition(rid).expect("first take returns Some");
        assert_eq!(taken.topic, "persistent://public/default/p");
        assert!(
            reg.take_partition(rid).is_none(),
            "duplicate take returns None"
        );
    }
}
