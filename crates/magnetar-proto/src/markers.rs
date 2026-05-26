// SPDX-License-Identifier: Apache-2.0

//! Decoder for PIP-33 `REPLICATED_SUBSCRIPTION_*` markers.
//!
//! Broker-driven snapshot/update markers travel inline with topic data, identified by
//! [`pb::MessageMetadata::marker_type`]. The client never originates them — the broker
//! generates the snapshot machinery as part of PIP-33's cross-cluster cursor synchronisation.
//! On the receive path, [`crate::Connection`] filters them out of the user-visible message
//! stream and emits [`crate::event::ConnectionEvent::ReplicatedSubscriptionMarkerObserved`]
//! instead (see [ADR-0034]).
//!
//! Two intentional design choices:
//!
//! - [`ReplicatedSubscriptionMarkerKind`] and [`ReplicatedSubscriptionMarkerDetails`] are
//!   `#[non_exhaustive]` so future upstream marker kinds / payload fields stay additive.
//! - The decoder returns `Ok(None)` for any `marker_type` that is not a replicated-subscription
//!   marker (including the txn marker family `20..=22` and any future unknown kind).
//!   Forward-compat: a broker that one day starts emitting a new marker kind will not break today's
//!   client; the receive-path filter logs and drops it.
//!
//! [ADR-0034]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0034-pip-33-replicated-subscriptions-scope.md

use prost::Message as _;

use crate::pb;

/// PIP-33 replicated-subscription marker kinds (`MarkerType` 10..=13).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ReplicatedSubscriptionMarkerKind {
    /// `REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST` (10).
    SnapshotRequest,
    /// `REPLICATED_SUBSCRIPTION_SNAPSHOT_RESPONSE` (11).
    SnapshotResponse,
    /// `REPLICATED_SUBSCRIPTION_SNAPSHOT` (12).
    Snapshot,
    /// `REPLICATED_SUBSCRIPTION_UPDATE` (13).
    Update,
}

impl ReplicatedSubscriptionMarkerKind {
    /// Map a raw `MessageMetadata.marker_type` integer to a marker kind.
    ///
    /// Returns `None` for `UNKNOWN_MARKER` (0), txn markers (20..=22) and any future
    /// marker kind this client does not yet understand.
    #[must_use]
    pub fn from_marker_type(marker_type: i32) -> Option<Self> {
        match marker_type {
            10 => Some(Self::SnapshotRequest),
            11 => Some(Self::SnapshotResponse),
            12 => Some(Self::Snapshot),
            13 => Some(Self::Update),
            _ => None,
        }
    }

    /// Inverse of [`Self::from_marker_type`].
    #[must_use]
    pub const fn marker_type(self) -> i32 {
        match self {
            Self::SnapshotRequest => 10,
            Self::SnapshotResponse => 11,
            Self::Snapshot => 12,
            Self::Update => 13,
        }
    }

    /// Stable wire name (matches the Pulsar `MarkerType` enum proto value).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SnapshotRequest => "REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST",
            Self::SnapshotResponse => "REPLICATED_SUBSCRIPTION_SNAPSHOT_RESPONSE",
            Self::Snapshot => "REPLICATED_SUBSCRIPTION_SNAPSHOT",
            Self::Update => "REPLICATED_SUBSCRIPTION_UPDATE",
        }
    }
}

/// A decoded PIP-33 marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicatedSubscriptionMarker {
    /// Marker kind (which protobuf payload was decoded).
    pub kind: ReplicatedSubscriptionMarkerKind,
    /// Decoded payload.
    pub details: ReplicatedSubscriptionMarkerDetails,
}

impl ReplicatedSubscriptionMarker {
    /// Convenience accessor: the `snapshot_id` field carried by SnapshotRequest, SnapshotResponse
    /// and Snapshot. `None` for Update (which carries a subscription name instead).
    #[must_use]
    pub fn snapshot_id(&self) -> Option<&str> {
        match &self.details {
            ReplicatedSubscriptionMarkerDetails::SnapshotRequest { snapshot_id, .. }
            | ReplicatedSubscriptionMarkerDetails::SnapshotResponse { snapshot_id, .. }
            | ReplicatedSubscriptionMarkerDetails::Snapshot { snapshot_id, .. } => {
                Some(snapshot_id.as_str())
            }
            ReplicatedSubscriptionMarkerDetails::Update { .. } => None,
        }
    }
}

/// Decoded payload of a replicated-subscription marker.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplicatedSubscriptionMarkerDetails {
    /// `REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST` — a peer cluster requested a snapshot.
    SnapshotRequest {
        /// Broker-assigned snapshot id correlating request → response → snapshot.
        snapshot_id: String,
        /// Source cluster name, if the broker populated it.
        source_cluster: Option<String>,
    },
    /// `REPLICATED_SUBSCRIPTION_SNAPSHOT_RESPONSE` — a peer cluster's reply.
    SnapshotResponse {
        /// Broker-assigned snapshot id.
        snapshot_id: String,
        /// Replying cluster's local cursor position.
        cluster: Option<ClusterMessageId>,
    },
    /// `REPLICATED_SUBSCRIPTION_SNAPSHOT` — the assembled cross-cluster snapshot.
    Snapshot {
        /// Broker-assigned snapshot id.
        snapshot_id: String,
        /// Local cluster's cursor position at snapshot time.
        local_message_id: Option<MarkersMessageIdData>,
        /// Peer-cluster cursor positions.
        clusters: Vec<ClusterMessageId>,
    },
    /// `REPLICATED_SUBSCRIPTION_UPDATE` — applies a cross-cluster cursor update to a subscription.
    Update {
        /// Target subscription name.
        subscription_name: String,
        /// Cluster cursor positions to install.
        clusters: Vec<ClusterMessageId>,
    },
}

/// Per-cluster cursor position carried inside replicated-subscription markers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterMessageId {
    /// Cluster name (matches the broker's `clusterName` config).
    pub cluster: String,
    /// Cursor position in that cluster's ledger.
    pub message_id: MarkersMessageIdData,
}

/// Compact `(ledger_id, entry_id)` cursor pair used by replicated-subscription markers.
///
/// Distinct from [`crate::types::MessageId`] because markers do not carry `partition_index`
/// or `batch_index` — they reference the broker-side ledger entry directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MarkersMessageIdData {
    /// BookKeeper ledger id.
    pub ledger_id: u64,
    /// Entry id inside the ledger.
    pub entry_id: u64,
}

impl From<pb::MarkersMessageIdData> for MarkersMessageIdData {
    fn from(value: pb::MarkersMessageIdData) -> Self {
        Self {
            ledger_id: value.ledger_id,
            entry_id: value.entry_id,
        }
    }
}

impl From<pb::ClusterMessageId> for ClusterMessageId {
    fn from(value: pb::ClusterMessageId) -> Self {
        Self {
            cluster: value.cluster,
            message_id: value.message_id.into(),
        }
    }
}

/// Decode failure for a replicated-subscription marker.
#[derive(Debug, thiserror::Error)]
pub enum MarkerDecodeError {
    /// `prost` rejected the payload bytes (truncated, wrong wire type, missing required field).
    #[error("replicated-subscription marker decode failed: {0}")]
    Protobuf(#[from] prost::DecodeError),
}

/// Decode a marker payload.
///
/// Returns:
/// - `Ok(Some(marker))` when `marker_type` is a replicated-subscription kind (10..=13) and the
///   payload decodes cleanly.
/// - `Ok(None)` when `marker_type` is `UNKNOWN_MARKER` (0), a txn marker (20..=22), or any future
///   kind not yet recognised by this client. The receive-path filter treats this as "leave the
///   existing path alone / log + drop" depending on the kind.
/// - `Err(_)` only when the payload bytes are present but malformed.
pub fn decode_replicated_subscription_marker(
    marker_type: i32,
    payload: &[u8],
) -> Result<Option<ReplicatedSubscriptionMarker>, MarkerDecodeError> {
    let Some(kind) = ReplicatedSubscriptionMarkerKind::from_marker_type(marker_type) else {
        return Ok(None);
    };
    let details = match kind {
        ReplicatedSubscriptionMarkerKind::SnapshotRequest => {
            let m = pb::ReplicatedSubscriptionsSnapshotRequest::decode(payload)?;
            ReplicatedSubscriptionMarkerDetails::SnapshotRequest {
                snapshot_id: m.snapshot_id,
                source_cluster: m.source_cluster,
            }
        }
        ReplicatedSubscriptionMarkerKind::SnapshotResponse => {
            let m = pb::ReplicatedSubscriptionsSnapshotResponse::decode(payload)?;
            ReplicatedSubscriptionMarkerDetails::SnapshotResponse {
                snapshot_id: m.snapshot_id,
                cluster: m.cluster.map(Into::into),
            }
        }
        ReplicatedSubscriptionMarkerKind::Snapshot => {
            let m = pb::ReplicatedSubscriptionsSnapshot::decode(payload)?;
            ReplicatedSubscriptionMarkerDetails::Snapshot {
                snapshot_id: m.snapshot_id,
                local_message_id: m.local_message_id.map(Into::into),
                clusters: m.clusters.into_iter().map(Into::into).collect(),
            }
        }
        ReplicatedSubscriptionMarkerKind::Update => {
            let m = pb::ReplicatedSubscriptionsUpdate::decode(payload)?;
            ReplicatedSubscriptionMarkerDetails::Update {
                subscription_name: m.subscription_name,
                clusters: m.clusters.into_iter().map(Into::into).collect(),
            }
        }
    };
    Ok(Some(ReplicatedSubscriptionMarker { kind, details }))
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    fn encode<M: Message>(m: &M) -> Vec<u8> {
        let mut buf = Vec::with_capacity(m.encoded_len());
        m.encode(&mut buf).unwrap();
        buf
    }

    #[test]
    fn marker_decode_snapshot_request_roundtrip() {
        let payload = encode(&pb::ReplicatedSubscriptionsSnapshotRequest {
            snapshot_id: "snap-1".to_owned(),
            source_cluster: Some("cluster-a".to_owned()),
        });
        let decoded = decode_replicated_subscription_marker(10, &payload)
            .unwrap()
            .unwrap();
        assert_eq!(
            decoded.kind,
            ReplicatedSubscriptionMarkerKind::SnapshotRequest
        );
        assert_eq!(decoded.snapshot_id(), Some("snap-1"));
        match decoded.details {
            ReplicatedSubscriptionMarkerDetails::SnapshotRequest {
                snapshot_id,
                source_cluster,
            } => {
                assert_eq!(snapshot_id, "snap-1");
                assert_eq!(source_cluster.as_deref(), Some("cluster-a"));
            }
            other => panic!("expected SnapshotRequest, got {other:?}"),
        }
    }

    #[test]
    fn marker_decode_snapshot_response_roundtrip() {
        let payload = encode(&pb::ReplicatedSubscriptionsSnapshotResponse {
            snapshot_id: "snap-2".to_owned(),
            cluster: Some(pb::ClusterMessageId {
                cluster: "cluster-b".to_owned(),
                message_id: pb::MarkersMessageIdData {
                    ledger_id: 7,
                    entry_id: 42,
                },
            }),
        });
        let decoded = decode_replicated_subscription_marker(11, &payload)
            .unwrap()
            .unwrap();
        assert_eq!(
            decoded.kind,
            ReplicatedSubscriptionMarkerKind::SnapshotResponse
        );
        match decoded.details {
            ReplicatedSubscriptionMarkerDetails::SnapshotResponse {
                snapshot_id,
                cluster,
            } => {
                assert_eq!(snapshot_id, "snap-2");
                let c = cluster.expect("cluster");
                assert_eq!(c.cluster, "cluster-b");
                assert_eq!(c.message_id.ledger_id, 7);
                assert_eq!(c.message_id.entry_id, 42);
            }
            other => panic!("expected SnapshotResponse, got {other:?}"),
        }
    }

    #[test]
    fn marker_decode_snapshot_roundtrip() {
        let payload = encode(&pb::ReplicatedSubscriptionsSnapshot {
            snapshot_id: "snap-3".to_owned(),
            local_message_id: Some(pb::MarkersMessageIdData {
                ledger_id: 1,
                entry_id: 2,
            }),
            clusters: vec![pb::ClusterMessageId {
                cluster: "peer".to_owned(),
                message_id: pb::MarkersMessageIdData {
                    ledger_id: 3,
                    entry_id: 4,
                },
            }],
        });
        let decoded = decode_replicated_subscription_marker(12, &payload)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.kind, ReplicatedSubscriptionMarkerKind::Snapshot);
        match decoded.details {
            ReplicatedSubscriptionMarkerDetails::Snapshot {
                snapshot_id,
                local_message_id,
                clusters,
            } => {
                assert_eq!(snapshot_id, "snap-3");
                let local = local_message_id.expect("local id");
                assert_eq!(local.ledger_id, 1);
                assert_eq!(local.entry_id, 2);
                assert_eq!(clusters.len(), 1);
                assert_eq!(clusters[0].cluster, "peer");
                assert_eq!(clusters[0].message_id.ledger_id, 3);
                assert_eq!(clusters[0].message_id.entry_id, 4);
            }
            other => panic!("expected Snapshot, got {other:?}"),
        }
    }

    #[test]
    fn marker_decode_update_roundtrip() {
        let payload = encode(&pb::ReplicatedSubscriptionsUpdate {
            subscription_name: "sub-x".to_owned(),
            clusters: vec![
                pb::ClusterMessageId {
                    cluster: "c1".to_owned(),
                    message_id: pb::MarkersMessageIdData {
                        ledger_id: 10,
                        entry_id: 20,
                    },
                },
                pb::ClusterMessageId {
                    cluster: "c2".to_owned(),
                    message_id: pb::MarkersMessageIdData {
                        ledger_id: 30,
                        entry_id: 40,
                    },
                },
            ],
        });
        let decoded = decode_replicated_subscription_marker(13, &payload)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.kind, ReplicatedSubscriptionMarkerKind::Update);
        assert_eq!(decoded.snapshot_id(), None);
        match decoded.details {
            ReplicatedSubscriptionMarkerDetails::Update {
                subscription_name,
                clusters,
            } => {
                assert_eq!(subscription_name, "sub-x");
                assert_eq!(clusters.len(), 2);
                assert_eq!(clusters[0].cluster, "c1");
                assert_eq!(clusters[1].cluster, "c2");
                assert_eq!(clusters[1].message_id.entry_id, 40);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn marker_decode_txn_marker_returns_none() {
        // Txn markers (kinds 20..=22) are not replicated-subscription markers and must be
        // ignored by this decoder. Forward-compat: any unknown kind also returns Ok(None).
        for txn_kind in [20i32, 21, 22] {
            assert!(
                decode_replicated_subscription_marker(txn_kind, b"")
                    .unwrap()
                    .is_none(),
                "kind {txn_kind} must decode to None"
            );
        }
        // UNKNOWN_MARKER and a hypothetical future kind also return None.
        assert!(
            decode_replicated_subscription_marker(0, b"")
                .unwrap()
                .is_none()
        );
        assert!(
            decode_replicated_subscription_marker(99, b"")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn malformed_payload_surfaces_decode_error() {
        // A replicated-subscription marker kind paired with truncated bytes must return Err.
        // `prost` rejects this because `snapshot_id` is required (proto2).
        let bad = decode_replicated_subscription_marker(10, b"\xff\xff\xff\xff");
        assert!(matches!(bad, Err(MarkerDecodeError::Protobuf(_))));
    }
}
