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
}
