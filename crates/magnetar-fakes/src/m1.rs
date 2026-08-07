// SPDX-License-Identifier: Apache-2.0

//! Stateful Pulsar 5.0.0-M1 scalable-topic fake cluster.
//!
//! The cluster is sans-I/O: callers open named endpoint connections, feed
//! encoded client frames with [`M1FakeCluster::handle_bytes`], and drain encoded
//! broker frames with [`M1FakeCluster::take_output`]. Controller membership and
//! segment delivery state are independent of Magnetar's client transitions, so
//! this fake can detect invalid routing, overlapping ownership, permit leaks,
//! and stale resource cleanup instead of echoing a client-authored transcript.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    Frame, FrameError, MAX_FRAME_SIZE, decode_one, encode_command, encode_payload, pb,
};

const HASH_RANGE_END: u32 = 65_535;
const MAX_FAKE_PAYLOAD_SIZE: usize = MAX_FRAME_SIZE - 1024;
const MAX_DAG_SEGMENTS: usize = 4096;
const MAX_DAG_EDGES: usize = 16_384;
const MAX_DAG_ANCESTRY_DEPTH: usize = 256;
const TRANSACTION_COORDINATOR_TOPIC: &str =
    "persistent://pulsar/system/transaction_coordinator_assign-partition-0";

/// A physical endpoint exposed by the fake cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Endpoint {
    /// The PIP-460 controller endpoint.
    Controller,
    /// A direct segment-broker endpoint. The number is a stable fake-broker id,
    /// not necessarily the id of the segment currently placed there.
    Segment(u64),
}

/// Transport selected for one physical fake connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransportSecurity {
    /// `pulsar://` authority.
    Plaintext,
    /// `pulsar+ssl://` authority.
    Tls,
}

/// Plaintext and TLS authorities advertised for one endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointAuthorities {
    /// Direct plaintext URL.
    pub plaintext: String,
    /// Direct TLS URL.
    pub tls: String,
}

impl EndpointAuthorities {
    /// Construct one endpoint's advertised authority pair.
    #[must_use]
    pub fn new(plaintext: impl Into<String>, tls: impl Into<String>) -> Self {
        Self {
            plaintext: plaintext.into(),
            tls: tls.into(),
        }
    }

    fn for_transport(&self, transport: TransportSecurity) -> &str {
        match transport {
            TransportSecurity::Plaintext => &self.plaintext,
            TransportSecurity::Tls => &self.tls,
        }
    }
}

/// Authentication material presented to a configurable validator.
///
/// Its `Debug` implementation redacts both fields; the fake passes borrowed
/// credential bytes to the validator and never stores or logs them.
pub struct AuthAttempt<'a> {
    /// Physical endpoint receiving `CONNECT`.
    pub endpoint: Endpoint,
    /// Transport used by that physical connection.
    pub transport: TransportSecurity,
    /// Optional Pulsar authentication method name.
    pub method: Option<&'a str>,
    /// Optional opaque authentication payload.
    pub data: Option<&'a [u8]>,
}

impl core::fmt::Debug for AuthAttempt<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("AuthAttempt")
            .field("endpoint", &self.endpoint)
            .field("transport", &self.transport)
            .field("method", &self.method.map(|_| "<redacted>"))
            .field("data", &self.data.map(|_| "<redacted>"))
            .finish()
    }
}

/// Non-logging authentication predicate used by the fake handshake.
pub type AuthValidator = Arc<dyn for<'a> Fn(AuthAttempt<'a>) -> bool + Send + Sync>;

/// Construction settings for a stateful M1 fake cluster.
#[derive(Clone)]
pub struct M1FakeConfig {
    topic: String,
    authorities: BTreeMap<Endpoint, EndpointAuthorities>,
    auth_validator: Option<AuthValidator>,
}

impl core::fmt::Debug for M1FakeConfig {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("M1FakeConfig")
            .field("topic", &self.topic)
            .field("authorities", &self.authorities)
            .field(
                "auth_validator",
                &self.auth_validator.as_ref().map(|_| "<redacted validator>"),
            )
            .finish()
    }
}

impl M1FakeConfig {
    /// Build the standard one-controller/two-segment authority set.
    pub fn new(topic: impl Into<String>) -> Result<Self, M1FakeError> {
        let topic = topic.into();
        validate_scalable_topic(&topic)?;
        Ok(Self {
            topic,
            authorities: default_authorities(),
            auth_validator: None,
        })
    }

    /// Replace one endpoint's plaintext/TLS authority pair.
    #[must_use]
    pub fn with_endpoint_authorities(
        mut self,
        endpoint: Endpoint,
        authorities: EndpointAuthorities,
    ) -> Self {
        self.authorities.insert(endpoint, authorities);
        self
    }

    /// Install a borrowed, non-logging authentication validator.
    #[must_use]
    pub fn with_auth_validator<F>(mut self, validator: F) -> Self
    where
        F: for<'a> Fn(AuthAttempt<'a>) -> bool + Send + Sync + 'static,
    {
        self.auth_validator = Some(Arc::new(validator));
        self
    }
}

/// One fake physical connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionId(pub u64);

/// A controller member or ordinary child consumer in one connection
/// incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemberId {
    /// Physical connection carrying this resource.
    pub connection: ConnectionId,
    /// Pulsar consumer id, scoped to the connection.
    pub consumer_id: u64,
}

impl MemberId {
    /// Build a member identity from a connection incarnation and consumer id.
    #[must_use]
    pub const fn new(connection: ConnectionId, consumer_id: u64) -> Self {
        Self {
            connection,
            consumer_id,
        }
    }
}

/// Identifier for an operation held by a scripted delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PendingOperationId(pub u64);

/// Operations whose next occurrence can be delayed or failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OperationKind {
    /// `CommandScalableTopicSubscribe` on the controller.
    ScalableOpen,
    /// Ordinary `CommandSubscribe` on a segment endpoint.
    SegmentOpen,
    /// Ordinary `CommandAck` on a segment endpoint.
    Ack,
    /// Ordinary `CommandSeek` on a segment endpoint.
    Seek,
    /// Ordinary `CommandCloseConsumer` on a segment endpoint.
    Close,
    /// Ordinary `CommandGetSchema` on a segment endpoint.
    GetSchema,
    /// `CommandAddSubscriptionToTxn` on the controller.
    TransactionRegistration,
    /// `CommandEndTxn` on the controller.
    EndTransaction,
}

/// A broker failure returned by a scripted operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerFailure {
    /// Vendored Pulsar server error.
    pub error: pb::ServerError,
    /// Stable diagnostic carried on the generated response frame.
    pub message: String,
}

impl BrokerFailure {
    /// Construct a scripted broker failure.
    #[must_use]
    pub fn new(error: pb::ServerError, message: impl Into<String>) -> Self {
        Self {
            error,
            message: message.into(),
        }
    }
}

/// Behavior consumed by the next matching operation on an endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptedBehavior {
    /// Hold the operation until [`M1FakeCluster::complete_pending`] is called.
    Delay,
    /// Fail immediately with a generated protocol response.
    Fail(BrokerFailure),
}

/// Resolution supplied for a delayed operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingCompletion {
    /// Commit the delayed operation and emit its success response.
    Succeed,
    /// Leave its durable state unchanged and emit a failure response.
    Fail(BrokerFailure),
}

/// One segment in a complete fake layout snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct M1Segment {
    /// M1 segment id.
    pub id: u64,
    /// Inclusive hash-range start.
    pub hash_start: u32,
    /// Inclusive hash-range end.
    pub hash_end: u32,
    /// Upstream two-state lifecycle.
    pub state: pb::SegmentState,
    /// Parent edges supplied by the broker.
    pub parent_ids: Vec<u64>,
    /// Child edges supplied by the broker.
    pub child_ids: Vec<u64>,
    /// Layout epoch at creation.
    pub created_at_epoch: u64,
    /// Layout epoch at sealing.
    pub sealed_at_epoch: Option<u64>,
    /// Bound serving route. Active segments require one and advertise it; a
    /// sealed segment may retain one for draining generations without advertising it.
    pub endpoint: Option<Endpoint>,
}

impl M1Segment {
    /// Construct an active, placed segment with no ancestry edges.
    #[must_use]
    pub fn active(
        id: u64,
        hash_start: u32,
        hash_end: u32,
        endpoint: Endpoint,
        created_at_epoch: u64,
    ) -> Self {
        Self {
            id,
            hash_start,
            hash_end,
            state: pb::SegmentState::Active,
            parent_ids: Vec::new(),
            child_ids: Vec::new(),
            created_at_epoch,
            sealed_at_epoch: None,
            endpoint: Some(endpoint),
        }
    }

    /// Attach parent edges to this segment.
    #[must_use]
    pub fn with_parents(mut self, parent_ids: impl IntoIterator<Item = u64>) -> Self {
        self.parent_ids = parent_ids.into_iter().collect();
        self
    }

    /// Attach child edges to this segment.
    #[must_use]
    pub fn with_children(mut self, child_ids: impl IntoIterator<Item = u64>) -> Self {
        self.child_ids = child_ids.into_iter().collect();
        self
    }

    /// Transition an active descriptor to sealed at `epoch`, retaining its
    /// serving placement for backlog drain.
    #[must_use]
    pub fn sealed_at(mut self, epoch: u64) -> Self {
        self.state = pb::SegmentState::Sealed;
        self.sealed_at_epoch = Some(epoch);
        self
    }

    fn to_pb(&self) -> pb::SegmentInfoProto {
        pb::SegmentInfoProto {
            segment_id: self.id,
            hash_start: self.hash_start,
            hash_end: self.hash_end,
            state: self.state as i32,
            parent_ids: self.parent_ids.clone(),
            child_ids: self.child_ids.clone(),
            created_at_epoch: self.created_at_epoch,
            sealed_at_epoch: self.sealed_at_epoch,
            created_at_ms: 0,
            sealed_at_ms: None,
            legacy_topic_name: None,
        }
    }
}

/// A member's complete legal segment share in one controller rebalance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullAssignment {
    /// Target controller member.
    pub member: MemberId,
    /// Complete set of segment ids assigned to the member after the rebalance.
    pub segments: Vec<u64>,
}

impl FullAssignment {
    /// Construct a complete assignment entry.
    #[must_use]
    pub fn new(member: MemberId, segments: impl IntoIterator<Item = u64>) -> Self {
        Self {
            member,
            segments: segments.into_iter().collect(),
        }
    }
}

/// Independent ownership evidence for a child segment's parent edges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AncestryProof {
    /// Every known parent and the child belonged to the same connection member.
    LocallyProvable {
        /// Member that owned both sides of the edge.
        member: MemberId,
        /// Parent ids named by the child.
        parent_ids: Vec<u64>,
    },
    /// At least one parent belonged to another member, so local completion
    /// cannot prove the ordering barrier.
    CrossMemberUnprovable {
        /// Member currently owning the child.
        child_member: MemberId,
        /// Distinct known owners of the child's parents.
        parent_members: Vec<MemberId>,
        /// Parent ids named by the child.
        parent_ids: Vec<u64>,
    },
    /// The fake has no ownership history for at least one parent edge.
    Unknown {
        /// Member currently owning the child.
        child_member: MemberId,
        /// Parent ids without broker-side ownership evidence.
        missing_parent_ids: Vec<u64>,
    },
}

/// Whether a member may grant FLOW to one assigned child under strict local
/// ancestry ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainEligibility {
    /// Every transitive predecessor is locally known complete.
    Eligible,
    /// Locally owned predecessors have not completed their terminal drain.
    ParentBlocked {
        /// Incomplete transitive predecessor ids.
        segment_ids: Vec<u64>,
    },
    /// At least one predecessor belongs to another controller member, so this
    /// member cannot prove completion locally.
    CrossMemberUnprovable {
        /// Predecessor ids whose last known owner differs.
        segment_ids: Vec<u64>,
    },
    /// An ancestry edge has no retained ownership evidence.
    UnknownAncestry {
        /// Predecessor ids without ownership evidence.
        segment_ids: Vec<u64>,
    },
}

/// One decoded client command and the physical destination that received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteObservation {
    /// Connection incarnation carrying the command.
    pub connection: ConnectionId,
    /// Actual endpoint that received the frame.
    pub endpoint: Endpoint,
    /// Generated `BaseCommand.Type` discriminator.
    pub command: pb::base_command::Type,
    /// Topic or resource id extracted without interpreting client transitions.
    pub resource: Option<String>,
}

/// One generated broker frame and the connection that received it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerFrameObservation {
    /// Connection receiving the generated frame.
    pub connection: ConnectionId,
    /// Generated `BaseCommand.Type` discriminator.
    pub command: pb::base_command::Type,
}

/// Broker-side lifecycle retained for one generated transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FakeTransactionState {
    /// Coordinator accepts registrations and transactional acknowledgements.
    Open,
    /// Commit was accepted and staged cursors were advanced.
    Committed,
    /// Abort was accepted and staged acknowledgements were redelivered.
    Aborted,
}

/// Read-only transaction state for independent assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionObservation {
    /// Generated transaction identifier.
    pub id: magnetar_proto::TxnId,
    /// Broker-side lifecycle.
    pub state: FakeTransactionState,
    /// Registered `(segment topic, subscription)` pairs.
    pub registered_subscriptions: Vec<(String, String)>,
    /// Transactional acknowledgements staged without cursor advancement.
    pub staged_acknowledgements: usize,
}

/// Read-only description of one delayed operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingOperationInfo {
    /// Stable pending-operation id.
    pub id: PendingOperationId,
    /// Connection that issued the operation.
    pub connection: ConnectionId,
    /// Physical endpoint that received it.
    pub endpoint: Endpoint,
    /// Operation class.
    pub kind: OperationKind,
    /// Request id when that command carries one.
    pub request_id: Option<u64>,
}

/// Observable cluster resources and accounting totals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceCounts {
    /// Open physical connections.
    pub connections: usize,
    /// Open controller connections.
    pub controller_connections: usize,
    /// Open segment connections.
    pub segment_connections: usize,
    /// Open scalable layout sessions.
    pub layout_sessions: usize,
    /// Registered scalable consumer members.
    pub scalable_members: usize,
    /// Scalable opens waiting for scripted completion.
    pub pending_scalable_opens: usize,
    /// Active ordinary segment consumers.
    pub child_consumers: usize,
    /// Exclusive child slots reserved by delayed opens.
    pub pending_child_opens: usize,
    /// All delayed operations.
    pub pending_operations: usize,
    /// Outstanding manual FLOW permits across child consumers.
    pub permits: u64,
    /// Messages retained in segment ledgers.
    pub ledger_messages: usize,
    /// Distinct delivered messages still awaiting acknowledgement.
    pub unacked_messages: usize,
}

/// Counts from one production sans-I/O connection exchange.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdapterExchange {
    /// Client bytes forwarded into the fake.
    pub client_bytes: usize,
    /// Generated broker frames fed through `Connection::handle_bytes`.
    pub broker_frames: usize,
}

/// Narrow bridge between a production [`magnetar_proto::Connection`] and one
/// logical fake endpoint connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct M1ConnectionAdapter {
    connection: ConnectionId,
}

impl M1ConnectionAdapter {
    /// Bind an adapter to a fake physical connection.
    #[must_use]
    pub const fn new(connection: ConnectionId) -> Self {
        Self { connection }
    }

    /// Drain the production connection's real outbound framing into the fake,
    /// then feed all resulting broker frames through the production decoder.
    pub fn exchange(
        self,
        cluster: &mut M1FakeCluster,
        client: &mut magnetar_proto::Connection,
        now: Instant,
    ) -> Result<AdapterExchange, M1AdapterError> {
        let mut outbound = BytesMut::new();
        match client.poll_transmit_owned() {
            magnetar_proto::TransmitOwned::Contiguous(bytes) => {
                outbound.extend_from_slice(&bytes);
            }
            magnetar_proto::TransmitOwned::Vectored(segments) => {
                for segment in segments {
                    outbound.extend_from_slice(&segment);
                }
            }
        }
        let client_bytes = outbound.len();
        if !outbound.is_empty() {
            let mut outbound = outbound.freeze();
            cluster.handle_bytes(self.connection, &mut outbound)?;
        }
        let broker_frames = cluster.take_output(self.connection)?;
        let count = broker_frames.len();
        for frame in broker_frames {
            client.handle_bytes(now, &frame)?;
        }
        Ok(AdapterExchange {
            client_bytes,
            broker_frames: count,
        })
    }
}

/// Production-connection bridge error.
#[derive(Debug, thiserror::Error)]
pub enum M1AdapterError {
    /// The fake rejected routing, ownership, or wire state.
    #[error(transparent)]
    Fake(#[from] M1FakeError),
    /// The production sans-I/O connection rejected a generated broker frame.
    #[error(transparent)]
    Protocol(#[from] magnetar_proto::ProtocolError),
}

/// Stateful fake-cluster error.
#[derive(Debug, thiserror::Error)]
pub enum M1FakeError {
    /// Wire framing or generated-protobuf encoding failed.
    #[error(transparent)]
    Frame(#[from] FrameError),
    /// A generated DAG failed the production model's validation.
    #[error(transparent)]
    GeneratedDag(#[from] magnetar_proto::DagValidationError),
    /// A generated assignment failed the production model's validation.
    #[error(transparent)]
    GeneratedAssignment(#[from] magnetar_proto::AssignmentError),
    /// The requested physical endpoint is not part of this cluster.
    #[error("unknown fake endpoint {0:?}")]
    UnknownEndpoint(Endpoint),
    /// The connection id is not owned by this cluster.
    #[error("unknown fake connection {0:?}")]
    UnknownConnection(ConnectionId),
    /// The connection has already been disconnected.
    #[error("fake connection {0:?} is disconnected")]
    Disconnected(ConnectionId),
    /// A command arrived before `CommandConnect` completed.
    #[error("command {command:?} arrived before CONNECT on {connection:?}")]
    HandshakeRequired {
        /// Connection receiving the command.
        connection: ConnectionId,
        /// Command that arrived too early.
        command: pb::base_command::Type,
    },
    /// A configured validator rejected the handshake without exposing
    /// authentication material in the error.
    #[error("authentication rejected for fake endpoint {endpoint:?}")]
    AuthenticationRejected {
        /// Endpoint whose validator rejected the credentials.
        endpoint: Endpoint,
    },
    /// A known command was sent to the wrong physical endpoint.
    #[error("command {command:?} reached {actual:?}, expected {expected:?}")]
    WrongEndpoint {
        /// Command being validated.
        command: pb::base_command::Type,
        /// Required endpoint.
        expected: Endpoint,
        /// Actual endpoint.
        actual: Endpoint,
    },
    /// The command discriminator is unsupported by this focused fake.
    #[error("unsupported fake-cluster command {0:?}")]
    UnsupportedCommand(pb::base_command::Type),
    /// The discriminator did not carry its generated command body or violated
    /// another command-local invariant.
    #[error("invalid {command:?}: {reason}")]
    InvalidCommand {
        /// Command being validated.
        command: pb::base_command::Type,
        /// Stable rejection reason.
        reason: String,
    },
    /// A complete layout failed independent broker-side validation.
    #[error("invalid fake M1 layout: {0}")]
    InvalidLayout(String),
    /// A complete assignment plan failed independent broker-side validation.
    #[error("invalid fake M1 assignment: {0}")]
    InvalidAssignment(String),
    /// A requested controller member does not exist.
    #[error("unknown scalable member {0:?}")]
    UnknownMember(MemberId),
    /// A requested segment does not exist in the fake's layout history.
    #[error("unknown fake segment {0}")]
    UnknownSegment(u64),
    /// A delayed operation id no longer exists, usually because its connection
    /// disconnected and cancelled it.
    #[error("unknown pending fake operation {0:?}")]
    UnknownPending(PendingOperationId),
    /// A delayed callback no longer matches the child generation/incarnation
    /// that issued it.
    #[error("stale pending fake operation {0:?}")]
    StalePending(PendingOperationId),
}

#[derive(Debug)]
struct ConnectionState {
    endpoint: Endpoint,
    transport: TransportSecurity,
    connected: bool,
    handshaken: bool,
    output: VecDeque<Bytes>,
}

#[derive(Debug, Clone)]
struct LayoutSnapshot {
    epoch: u64,
    segments: BTreeMap<u64, M1Segment>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GroupKey {
    topic: String,
    subscription: String,
}

#[derive(Debug)]
struct Membership {
    group: GroupKey,
    consumer_name: String,
    registered: bool,
    assignment_epoch: u64,
    assigned: BTreeSet<u64>,
}

#[derive(Debug, Clone)]
struct Baseline {
    epoch: u64,
    segments: BTreeSet<u64>,
}

#[derive(Debug, Clone)]
struct AssignmentContext {
    legal_segments: BTreeSet<u64>,
    consumer_names: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ChildSubscriptionKey {
    topic: String,
    subscription: String,
}

#[derive(Debug, Clone, Copy)]
enum ChildOwner {
    Pending(PendingOperationId),
    Active(MemberId),
}

#[derive(Debug, Default)]
struct ChildSubscriptionState {
    owner: Option<ChildOwner>,
    durable_cursor: Option<usize>,
    individually_acked: BTreeSet<usize>,
    next_generation: u64,
}

#[derive(Debug)]
struct ChildConsumer {
    segment_id: u64,
    group: GroupKey,
    key: ChildSubscriptionKey,
    controller_member: MemberId,
    serving_endpoint: Endpoint,
    generation: u64,
    delivery_cursor: usize,
    permits: u32,
    unacked: BTreeSet<usize>,
    redeliver: VecDeque<usize>,
    redelivery_counts: BTreeMap<usize, u32>,
    closing: bool,
    terminal_sent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChildFence {
    segment_id: u64,
    key: ChildSubscriptionKey,
    controller_member: MemberId,
    serving_endpoint: Endpoint,
    generation: u64,
    child_incarnation: ConnectionId,
}

#[derive(Debug)]
struct ValidatedChildOpen {
    member: MemberId,
    segment_id: u64,
    group: GroupKey,
    controller_member: MemberId,
    serving_endpoint: Endpoint,
    key: ChildSubscriptionKey,
}

#[derive(Debug, Clone)]
struct ChildActivation {
    member: MemberId,
    segment_id: u64,
    group: GroupKey,
    controller_member: MemberId,
    serving_endpoint: Endpoint,
    generation: u64,
    key: ChildSubscriptionKey,
    cursor: usize,
}

#[derive(Debug, Clone)]
struct StoredMessage {
    payload: Bytes,
    metadata: pb::MessageMetadata,
    ack_set: Vec<i64>,
}

#[derive(Debug, Clone)]
struct StagedTransactionalAck {
    member: MemberId,
    ack_type: pb::command_ack::AckType,
    indices: Vec<usize>,
    fence: ChildFence,
}

#[derive(Debug)]
struct FakeTransaction {
    state: FakeTransactionState,
    registered_subscriptions: BTreeSet<ChildSubscriptionKey>,
    staged_acknowledgements: Vec<StagedTransactionalAck>,
}

#[derive(Debug)]
enum PendingOperation {
    ScalableOpen {
        info: PendingOperationInfo,
        member: MemberId,
    },
    SegmentOpen {
        info: PendingOperationInfo,
        command: Box<pb::CommandSubscribe>,
        activation: ChildActivation,
    },
    Ack {
        info: PendingOperationInfo,
        member: MemberId,
        ack_type: pb::command_ack::AckType,
        indices: Vec<usize>,
        request_id: Option<u64>,
        fence: ChildFence,
        txn_id: Option<magnetar_proto::TxnId>,
    },
    Seek {
        info: PendingOperationInfo,
        member: MemberId,
        request_id: u64,
        target: usize,
        fence: ChildFence,
    },
    Close {
        info: PendingOperationInfo,
        member: MemberId,
        request_id: u64,
        fence: ChildFence,
    },
    GetSchema {
        info: PendingOperationInfo,
        request_id: u64,
        topic: String,
        schema_version: Option<Bytes>,
    },
    TransactionRegistration {
        info: PendingOperationInfo,
        txn_id: magnetar_proto::TxnId,
        key: ChildSubscriptionKey,
        request_id: u64,
    },
    EndTransaction {
        info: PendingOperationInfo,
        txn_id: magnetar_proto::TxnId,
        request_id: u64,
        action: pb::TxnAction,
    },
}

impl PendingOperation {
    fn info(&self) -> &PendingOperationInfo {
        match self {
            Self::ScalableOpen { info, .. }
            | Self::SegmentOpen { info, .. }
            | Self::Ack { info, .. }
            | Self::Seek { info, .. }
            | Self::Close { info, .. }
            | Self::GetSchema { info, .. }
            | Self::TransactionRegistration { info, .. }
            | Self::EndTransaction { info, .. } => info,
        }
    }
}

/// Independently stateful one-controller, two-segment-endpoint M1 fake cluster.
pub struct M1FakeCluster {
    topic: String,
    authorities: BTreeMap<Endpoint, EndpointAuthorities>,
    auth_validator: Option<AuthValidator>,
    current_layout: LayoutSnapshot,
    layout_history: BTreeMap<u64, LayoutSnapshot>,
    segment_catalog: BTreeMap<u64, M1Segment>,
    connections: BTreeMap<ConnectionId, ConnectionState>,
    next_connection_id: u64,
    layout_sessions: BTreeSet<(ConnectionId, u64)>,
    memberships: BTreeMap<MemberId, Membership>,
    baselines: BTreeMap<(GroupKey, String), Baseline>,
    assignment_contexts: BTreeMap<(GroupKey, u64), AssignmentContext>,
    segment_owners: BTreeMap<(GroupKey, u64), MemberId>,
    ownership_history: BTreeMap<(GroupKey, u64), MemberId>,
    completed_segments: BTreeSet<(GroupKey, u64)>,
    child_subscriptions: BTreeMap<ChildSubscriptionKey, ChildSubscriptionState>,
    child_consumers: BTreeMap<MemberId, ChildConsumer>,
    closed_consumers: BTreeSet<MemberId>,
    ledgers: BTreeMap<u64, Vec<StoredMessage>>,
    terminal_segments: BTreeSet<u64>,
    transactions: BTreeMap<magnetar_proto::TxnId, FakeTransaction>,
    next_transaction_sequence: u64,
    scripts: BTreeMap<(Endpoint, OperationKind), VecDeque<ScriptedBehavior>>,
    pending: BTreeMap<PendingOperationId, PendingOperation>,
    next_pending_id: u64,
    routes: Vec<RouteObservation>,
    broker_frames: Vec<BrokerFrameObservation>,
}

impl core::fmt::Debug for M1FakeCluster {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("M1FakeCluster")
            .field("topic", &self.topic)
            .field("authorities", &self.authorities)
            .field(
                "auth_validator",
                &self.auth_validator.as_ref().map(|_| "<redacted validator>"),
            )
            .field("layout_epoch", &self.current_layout.epoch)
            .field("connections", &self.connections.len())
            .field("memberships", &self.memberships.len())
            .field("child_consumers", &self.child_consumers.len())
            .field("transactions", &self.transactions.len())
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl Default for M1FakeCluster {
    fn default() -> Self {
        Self::two_segment()
    }
}

impl M1FakeCluster {
    /// Construct the standard fixture for `topic://public/default/scaled`.
    ///
    /// Its active inclusive ranges are `[0, 32767]` and `[32768, 65535]`,
    /// placed on two distinct child endpoints at layout epoch 1.
    #[must_use]
    pub fn two_segment() -> Self {
        Self::from_config_unchecked(M1FakeConfig {
            topic: "topic://public/default/scaled".to_owned(),
            authorities: default_authorities(),
            auth_validator: None,
        })
    }

    /// Construct the standard two-segment fixture for another scalable topic.
    pub fn for_topic(topic: impl Into<String>) -> Result<Self, M1FakeError> {
        Self::from_config(M1FakeConfig::new(topic)?)
    }

    /// Construct a cluster with configurable endpoint authorities and
    /// non-logging handshake validation.
    pub fn from_config(config: M1FakeConfig) -> Result<Self, M1FakeError> {
        validate_scalable_topic(&config.topic)?;
        for endpoint in [
            Endpoint::Controller,
            Endpoint::Segment(1),
            Endpoint::Segment(2),
        ] {
            let authorities = config
                .authorities
                .get(&endpoint)
                .ok_or(M1FakeError::UnknownEndpoint(endpoint))?;
            validate_authorities(endpoint, authorities)?;
        }
        for (endpoint, authorities) in &config.authorities {
            validate_authorities(*endpoint, authorities)?;
        }
        Ok(Self::from_config_unchecked(config))
    }

    fn from_config_unchecked(config: M1FakeConfig) -> Self {
        let segments = BTreeMap::from([
            (1, M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)),
            (
                2,
                M1Segment::active(2, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 0),
            ),
        ]);
        let current_layout = LayoutSnapshot { epoch: 1, segments };
        let layout_history = BTreeMap::from([(1, current_layout.clone())]);
        let segment_catalog = current_layout.segments.clone();
        Self {
            topic: config.topic,
            authorities: config.authorities,
            auth_validator: config.auth_validator,
            current_layout,
            layout_history,
            segment_catalog,
            connections: BTreeMap::new(),
            next_connection_id: 1,
            layout_sessions: BTreeSet::new(),
            memberships: BTreeMap::new(),
            baselines: BTreeMap::new(),
            assignment_contexts: BTreeMap::new(),
            segment_owners: BTreeMap::new(),
            ownership_history: BTreeMap::new(),
            completed_segments: BTreeSet::new(),
            child_subscriptions: BTreeMap::new(),
            child_consumers: BTreeMap::new(),
            closed_consumers: BTreeSet::new(),
            ledgers: BTreeMap::new(),
            terminal_segments: BTreeSet::new(),
            transactions: BTreeMap::new(),
            next_transaction_sequence: 1,
            scripts: BTreeMap::new(),
            pending: BTreeMap::new(),
            next_pending_id: 1,
            routes: Vec::new(),
            broker_frames: Vec::new(),
        }
    }

    /// Scalable topic served by this fixture.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Resolve the current controller member for a stable subscription and
    /// consumer name. Includes a delayed, not-yet-registered scalable open so
    /// tests can publish a push before its subscribe response.
    #[must_use]
    pub fn member(&self, subscription: &str, consumer_name: &str) -> Option<MemberId> {
        self.memberships.iter().find_map(|(member, membership)| {
            (membership.group.topic == self.topic
                && membership.group.subscription == subscription
                && membership.consumer_name == consumer_name)
                .then_some(*member)
        })
    }

    /// Open layout-watch session ids in deterministic connection/session order.
    #[must_use]
    pub fn layout_session_ids(&self) -> Vec<(ConnectionId, u64)> {
        self.layout_sessions.iter().copied().collect()
    }

    /// Current layout epoch.
    #[must_use]
    pub const fn layout_epoch(&self) -> u64 {
        self.current_layout.epoch
    }

    /// Decode the current generated wire DAG into the public sans-I/O model.
    pub fn dag_snapshot(&self) -> Result<magnetar_proto::DagSnapshot, M1FakeError> {
        magnetar_proto::DagSnapshot::try_from_pb(&self.layout_pb(&self.current_layout))
            .map_err(M1FakeError::from)
    }

    /// Decode one generated segment set into a public consumer assignment.
    pub fn consumer_assignment(
        &self,
        layout_epoch: u64,
        segments: impl IntoIterator<Item = u64>,
    ) -> Result<magnetar_proto::ConsumerAssignment, M1FakeError> {
        let group = GroupKey {
            topic: self.topic.clone(),
            subscription: "model-projection".to_owned(),
        };
        let segments = segments.into_iter().collect();
        let assignment = self.assignment_pb(&group, layout_epoch, &segments)?;
        magnetar_proto::ConsumerAssignment::try_from_pb(&assignment, &self.topic)
            .map_err(M1FakeError::from)
    }

    /// Stable plaintext fake service URL for an endpoint.
    #[must_use]
    pub fn endpoint_url(&self, endpoint: Endpoint) -> Option<&str> {
        self.endpoint_url_for(endpoint, TransportSecurity::Plaintext)
    }

    /// Stable fake service URL for an endpoint and transport.
    #[must_use]
    pub fn endpoint_url_for(
        &self,
        endpoint: Endpoint,
        transport: TransportSecurity,
    ) -> Option<&str> {
        self.authorities
            .get(&endpoint)
            .map(|authorities| authorities.for_transport(transport))
    }

    /// Canonical M1 attachment for a segment id in layout history.
    #[must_use]
    pub fn segment_topic(&self, segment_id: u64) -> Option<String> {
        self.segment_catalog
            .get(&segment_id)
            .and_then(|segment| canonical_segment_topic(&self.topic, segment).ok())
    }

    /// Current physical serving placement of a segment.
    #[must_use]
    pub fn segment_endpoint(&self, segment_id: u64) -> Option<Endpoint> {
        self.current_layout
            .segments
            .get(&segment_id)
            .and_then(|segment| segment.endpoint)
    }

    /// Open a physical connection. The caller must next send `CommandConnect`.
    pub fn open_connection(&mut self, endpoint: Endpoint) -> Result<ConnectionId, M1FakeError> {
        self.open_connection_with_transport(endpoint, TransportSecurity::Plaintext)
    }

    /// Open a physical connection using an explicit plaintext/TLS transport.
    pub fn open_connection_with_transport(
        &mut self,
        endpoint: Endpoint,
        transport: TransportSecurity,
    ) -> Result<ConnectionId, M1FakeError> {
        if !self.authorities.contains_key(&endpoint) {
            return Err(M1FakeError::UnknownEndpoint(endpoint));
        }
        let id = ConnectionId(self.next_connection_id);
        self.next_connection_id = self.next_connection_id.saturating_add(1);
        self.connections.insert(
            id,
            ConnectionState {
                endpoint,
                transport,
                connected: true,
                handshaken: false,
                output: VecDeque::new(),
            },
        );
        Ok(id)
    }

    /// Physically disconnect one connection and release all connection-scoped
    /// controller and child resources. Durable child cursors and controller
    /// reconnect baselines survive.
    pub fn disconnect_connection(&mut self, connection: ConnectionId) -> Result<(), M1FakeError> {
        let state = self
            .connections
            .get_mut(&connection)
            .ok_or(M1FakeError::UnknownConnection(connection))?;
        if !state.connected {
            return Err(M1FakeError::Disconnected(connection));
        }
        state.connected = false;
        state.output.clear();

        self.layout_sessions
            .retain(|(candidate, _)| *candidate != connection);

        let pending_ids: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(id, operation)| {
                (operation.info().connection == connection).then_some(*id)
            })
            .collect();
        for pending_id in pending_ids {
            self.cancel_pending(pending_id);
        }

        let controller_members: Vec<_> = self
            .memberships
            .keys()
            .filter(|member| member.connection == connection)
            .copied()
            .collect();
        for member in controller_members {
            self.remove_membership(member, true);
        }

        let child_members: Vec<_> = self
            .child_consumers
            .keys()
            .filter(|member| member.connection == connection)
            .copied()
            .collect();
        for member in child_members {
            self.remove_child_consumer(member);
        }
        Ok(())
    }

    /// Disconnect every open connection to one endpoint.
    pub fn disconnect_endpoint(&mut self, endpoint: Endpoint) -> Result<usize, M1FakeError> {
        if !self.authorities.contains_key(&endpoint) {
            return Err(M1FakeError::UnknownEndpoint(endpoint));
        }
        let connections: Vec<_> = self
            .connections
            .iter()
            .filter_map(|(id, state)| {
                (state.connected && state.endpoint == endpoint).then_some(*id)
            })
            .collect();
        for connection in &connections {
            self.disconnect_connection(*connection)?;
        }
        Ok(connections.len())
    }

    /// Feed one or more complete generated client frames to a connection.
    pub fn handle_bytes(
        &mut self,
        connection: ConnectionId,
        bytes: &mut Bytes,
    ) -> Result<(), M1FakeError> {
        self.connected_state(connection)?;
        while !bytes.is_empty() {
            let frame = decode_one(bytes)?;
            self.handle_frame(connection, &frame)?;
        }
        Ok(())
    }

    /// Drain generated broker frames queued for a connection, preserving order.
    pub fn take_output(&mut self, connection: ConnectionId) -> Result<Vec<Bytes>, M1FakeError> {
        let state = self.connected_state_mut(connection)?;
        Ok(state.output.drain(..).collect())
    }

    /// Script the next matching operation on an endpoint.
    pub fn script_next(
        &mut self,
        endpoint: Endpoint,
        kind: OperationKind,
        behavior: ScriptedBehavior,
    ) -> Result<(), M1FakeError> {
        if !self.authorities.contains_key(&endpoint) {
            return Err(M1FakeError::UnknownEndpoint(endpoint));
        }
        self.scripts
            .entry((endpoint, kind))
            .or_default()
            .push_back(behavior);
        Ok(())
    }

    /// Complete a delayed open, acknowledgement, or close.
    #[allow(clippy::too_many_lines)]
    pub fn complete_pending(
        &mut self,
        id: PendingOperationId,
        completion: PendingCompletion,
    ) -> Result<(), M1FakeError> {
        let operation = self
            .pending
            .remove(&id)
            .ok_or(M1FakeError::UnknownPending(id))?;
        match operation {
            PendingOperation::ScalableOpen { info, member } => match completion {
                PendingCompletion::Succeed => self.register_membership(member).and_then(|()| {
                    self.queue_scalable_subscribe_response(
                        member,
                        info.request_id.unwrap_or(0),
                        None,
                    )
                }),
                PendingCompletion::Fail(failure) => {
                    self.remove_membership(member, false);
                    self.queue_scalable_subscribe_failure(
                        info.connection,
                        info.request_id.unwrap_or(0),
                        &failure,
                    )
                }
            },
            PendingOperation::SegmentOpen {
                info,
                command,
                activation,
            } => {
                if !self.pending_open_is_current(id, &activation) {
                    self.release_pending_child_owner(&activation.key, id);
                    return Err(M1FakeError::StalePending(id));
                }
                match completion {
                    PendingCompletion::Succeed => {
                        let member = activation.member;
                        self.activate_child_consumer(activation);
                        self.queue_success(info.connection, command.request_id)
                            .and_then(|()| self.maybe_emit_terminal(member))
                    }
                    PendingCompletion::Fail(failure) => {
                        self.release_pending_child_owner(&activation.key, id);
                        self.queue_error(info.connection, command.request_id, &failure)
                    }
                }
            }
            PendingOperation::Ack {
                info,
                member,
                ack_type,
                indices,
                request_id,
                fence,
                txn_id,
            } => self.complete_pending_ack(
                id, &info, member, ack_type, &indices, request_id, &fence, txn_id, completion,
            ),
            PendingOperation::Seek {
                info,
                member,
                request_id,
                target,
                fence,
            } => match completion {
                PendingCompletion::Succeed => self
                    .require_child_fence(id, member, &fence)
                    .and_then(|()| self.apply_seek(member, target))
                    .and_then(|()| self.queue_success(info.connection, request_id)),
                PendingCompletion::Fail(failure) => self
                    .require_child_fence(id, member, &fence)
                    .and_then(|()| self.queue_error(info.connection, request_id, &failure)),
            },
            PendingOperation::Close {
                info,
                member,
                request_id,
                fence,
            } => match completion {
                PendingCompletion::Succeed => {
                    self.require_close_fence(id, member, &fence).and_then(|()| {
                        self.remove_child_consumer(member);
                        self.queue_success(info.connection, request_id)
                    })
                }
                PendingCompletion::Fail(failure) => {
                    self.require_close_fence(id, member, &fence).and_then(|()| {
                        if let Some(consumer) = self.child_consumers.get_mut(&member) {
                            consumer.closing = false;
                        }
                        self.queue_error(info.connection, request_id, &failure)
                    })
                }
            },
            PendingOperation::GetSchema {
                info,
                request_id,
                topic,
                schema_version,
            } => self.queue_get_schema_response(
                info.connection,
                request_id,
                topic,
                schema_version,
                match &completion {
                    PendingCompletion::Succeed => None,
                    PendingCompletion::Fail(failure) => Some(failure),
                },
            ),
            PendingOperation::TransactionRegistration {
                info,
                txn_id,
                key,
                request_id,
            } => self.complete_pending_transaction_registration(
                &info, txn_id, &key, request_id, completion,
            ),
            PendingOperation::EndTransaction {
                info,
                txn_id,
                request_id,
                action,
            } => match completion {
                PendingCompletion::Succeed => {
                    self.complete_end_transaction(info.connection, request_id, txn_id, action)
                }
                PendingCompletion::Fail(failure) => self.queue_end_transaction_response(
                    info.connection,
                    request_id,
                    txn_id,
                    Some(&failure),
                ),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn complete_pending_ack(
        &mut self,
        id: PendingOperationId,
        info: &PendingOperationInfo,
        member: MemberId,
        ack_type: pb::command_ack::AckType,
        indices: &[usize],
        request_id: Option<u64>,
        fence: &ChildFence,
        txn_id: Option<magnetar_proto::TxnId>,
        completion: PendingCompletion,
    ) -> Result<(), M1FakeError> {
        self.require_child_fence(id, member, fence)?;
        let result = match completion {
            PendingCompletion::Succeed => self
                .apply_or_stage_ack(member, ack_type, indices, fence, txn_id)
                .and_then(|()| {
                    self.queue_ack_response(
                        info.connection,
                        member.consumer_id,
                        request_id,
                        txn_id,
                        None,
                    )
                })
                .and_then(|()| self.maybe_emit_terminal(member)),
            PendingCompletion::Fail(failure) => self.queue_ack_response(
                info.connection,
                member.consumer_id,
                request_id,
                txn_id,
                Some(&failure),
            ),
        };
        result.map(|()| self.refresh_segment_completion(member))
    }

    fn complete_pending_transaction_registration(
        &mut self,
        info: &PendingOperationInfo,
        txn_id: magnetar_proto::TxnId,
        key: &ChildSubscriptionKey,
        request_id: u64,
        completion: PendingCompletion,
    ) -> Result<(), M1FakeError> {
        match completion {
            PendingCompletion::Succeed => {
                self.register_transaction_subscription(txn_id, key)?;
                self.queue_add_subscription_response(info.connection, request_id, txn_id, None)
            }
            PendingCompletion::Fail(failure) => self.queue_add_subscription_response(
                info.connection,
                request_id,
                txn_id,
                Some(&failure),
            ),
        }
    }

    /// Snapshot delayed operations in deterministic id order.
    #[must_use]
    pub fn pending_operations(&self) -> Vec<PendingOperationInfo> {
        self.pending
            .values()
            .map(|operation| operation.info().clone())
            .collect()
    }

    /// Replace the current layout with a validated, strictly newer complete
    /// snapshot and push it to every open layout session. A snapshot may garbage
    /// collect sealed nodes and remove only their adjacent retained edges.
    pub fn advance_layout(
        &mut self,
        epoch: u64,
        segments: Vec<M1Segment>,
    ) -> Result<(), M1FakeError> {
        if epoch <= self.current_layout.epoch {
            return Err(M1FakeError::InvalidLayout(format!(
                "epoch {epoch} does not advance {}",
                self.current_layout.epoch
            )));
        }
        let segments = self.validate_layout(epoch, segments)?;
        for segment in segments.values() {
            self.segment_catalog.insert(segment.id, segment.clone());
        }
        let snapshot = LayoutSnapshot { epoch, segments };
        self.current_layout = snapshot.clone();
        self.layout_history.insert(epoch, snapshot.clone());
        let sessions: Vec<_> = self.layout_sessions.iter().copied().collect();
        for (connection, session_id) in sessions {
            self.queue_layout(connection, session_id, &snapshot)?;
        }
        Ok(())
    }

    /// Re-send the current layout to one session without mutating broker state.
    pub fn resend_layout(
        &mut self,
        connection: ConnectionId,
        session_id: u64,
    ) -> Result<(), M1FakeError> {
        self.require_layout_session(connection, session_id)?;
        let snapshot = self.current_layout.clone();
        self.queue_layout(connection, session_id, &snapshot)
    }

    /// Terminate one live layout watch with a generated broker failure.
    pub fn fail_layout_session(
        &mut self,
        connection: ConnectionId,
        session_id: u64,
        failure: BrokerFailure,
    ) -> Result<(), M1FakeError> {
        self.require_layout_session(connection, session_id)?;
        self.layout_sessions.remove(&(connection, session_id));
        self.queue_command(
            connection,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
                scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
                    session_id,
                    dag: None,
                    error: Some(failure.error as i32),
                    message: Some(failure.message),
                    resolved_topic_name: None,
                }),
                ..Default::default()
            },
        )
    }

    /// Push a historical layout to one session without rolling broker state
    /// backward. The requested epoch must be older than the current layout.
    pub fn push_stale_layout(
        &mut self,
        connection: ConnectionId,
        session_id: u64,
        epoch: u64,
    ) -> Result<(), M1FakeError> {
        self.require_layout_session(connection, session_id)?;
        if epoch >= self.current_layout.epoch {
            return Err(M1FakeError::InvalidLayout(format!(
                "epoch {epoch} is not stale relative to {}",
                self.current_layout.epoch
            )));
        }
        let snapshot = self.layout_history.get(&epoch).cloned().ok_or_else(|| {
            M1FakeError::InvalidLayout(format!("unknown historical epoch {epoch}"))
        })?;
        self.queue_layout(connection, session_id, &snapshot)
    }

    /// Apply a complete controller assignment plan and push one full generated
    /// assignment to every member in the subscription group.
    ///
    /// A changed plan at the current layout epoch is authoritative. A complete
    /// historical plan is emitted to every member for stale-frame tests without
    /// mutating ownership; [`Self::push_stale_assignment`] emits one member's
    /// historical share.
    pub fn publish_assignment_plan(
        &mut self,
        layout_epoch: u64,
        plan: Vec<FullAssignment>,
    ) -> Result<(), M1FakeError> {
        let (group, assignment_segments) = self.validate_assignment_plan(layout_epoch, &plan)?;
        if layout_epoch < self.current_layout.epoch {
            return self.queue_historical_assignment_plan(&group, layout_epoch, &plan);
        }
        self.retain_assignment_context(&group, layout_epoch, &plan, &assignment_segments);
        self.install_assignment_plan(&group, layout_epoch, &plan)?;
        for entry in plan {
            self.queue_assignment_update(entry.member)?;
        }
        Ok(())
    }

    /// Publish a complete current-epoch assignment that includes active
    /// descendants before their sealed parents finish draining.
    ///
    /// This is the broker-valid adversarial shape needed to verify that a
    /// client may attach descendants early but must enforce its own strict
    /// FLOW barrier. All normal membership, uniqueness, coverage, and segment
    /// identity validation remains in force; only the completion-derived
    /// assignment frontier used by [`Self::publish_assignment_plan`] is
    /// replaced with the complete current DAG.
    pub fn publish_early_descendant_assignment_plan(
        &mut self,
        layout_epoch: u64,
        plan: Vec<FullAssignment>,
    ) -> Result<(), M1FakeError> {
        if layout_epoch != self.current_layout.epoch {
            return Err(M1FakeError::InvalidAssignment(format!(
                "early-descendant assignment epoch {layout_epoch} does not match current layout {}",
                self.current_layout.epoch
            )));
        }
        let group = self.validate_assignment_plan_members(layout_epoch, &plan)?;
        let assignment_segments = self.current_layout.segments.keys().copied().collect();
        self.validate_assignment_plan_segments(layout_epoch, &plan, &assignment_segments)?;
        self.retain_assignment_context(&group, layout_epoch, &plan, &assignment_segments);
        self.install_assignment_plan(&group, layout_epoch, &plan)?;
        for entry in plan {
            self.queue_assignment_update(entry.member)?;
        }
        Ok(())
    }

    fn validate_assignment_plan(
        &self,
        layout_epoch: u64,
        plan: &[FullAssignment],
    ) -> Result<(GroupKey, BTreeSet<u64>), M1FakeError> {
        let group = self.validate_assignment_plan_members(layout_epoch, plan)?;
        let assignment_segments = self.assignment_segment_ids(layout_epoch, &group);
        self.validate_assignment_plan_segments(layout_epoch, plan, &assignment_segments)?;
        Ok((group, assignment_segments))
    }

    fn validate_assignment_plan_members(
        &self,
        layout_epoch: u64,
        plan: &[FullAssignment],
    ) -> Result<GroupKey, M1FakeError> {
        if plan.is_empty() {
            return Err(M1FakeError::InvalidAssignment(
                "a complete plan must name at least one member".to_owned(),
            ));
        }
        if layout_epoch > self.current_layout.epoch {
            return Err(M1FakeError::InvalidAssignment(format!(
                "assignment epoch {layout_epoch} is ahead of layout {}",
                self.current_layout.epoch
            )));
        }
        let first = self
            .memberships
            .get(&plan[0].member)
            .ok_or(M1FakeError::UnknownMember(plan[0].member))?;
        let group = first.group.clone();
        let group_members: BTreeSet<_> = self
            .memberships
            .iter()
            .filter_map(|(member, membership)| (membership.group == group).then_some(*member))
            .collect();
        let plan_members: BTreeSet<_> = plan.iter().map(|entry| entry.member).collect();
        if plan_members.len() != plan.len() {
            return Err(M1FakeError::InvalidAssignment(
                "a plan must not repeat a group member".to_owned(),
            ));
        }
        if layout_epoch < self.current_layout.epoch {
            let context = self
                .assignment_contexts
                .get(&(group.clone(), layout_epoch))
                .ok_or_else(|| {
                    M1FakeError::InvalidAssignment(format!(
                        "assignment has no retained context for historical layout epoch {layout_epoch}"
                    ))
                })?;
            let plan_names: BTreeSet<_> = plan
                .iter()
                .map(|entry| {
                    self.memberships
                        .get(&entry.member)
                        .filter(|membership| membership.group == group)
                        .map(|membership| membership.consumer_name.clone())
                        .ok_or(M1FakeError::UnknownMember(entry.member))
                })
                .collect::<Result<_, _>>()?;
            if !plan_names.is_subset(&context.consumer_names) {
                return Err(M1FakeError::InvalidAssignment(
                    "a historical plan names a consumer identity absent at that epoch".to_owned(),
                ));
            }
        } else if plan_members != group_members {
            return Err(M1FakeError::InvalidAssignment(
                "a current plan must name every group member exactly once".to_owned(),
            ));
        }
        Ok(group)
    }

    fn validate_assignment_plan_segments(
        &self,
        layout_epoch: u64,
        plan: &[FullAssignment],
        assignment_segments: &BTreeSet<u64>,
    ) -> Result<(), M1FakeError> {
        let mut assigned_once = BTreeSet::new();
        for entry in plan {
            let local: BTreeSet<_> = entry.segments.iter().copied().collect();
            if local.len() != entry.segments.len() {
                return Err(M1FakeError::InvalidAssignment(format!(
                    "member {:?} repeats a segment",
                    entry.member
                )));
            }
            for segment_id in &local {
                if !assignment_segments.contains(segment_id) {
                    return Err(M1FakeError::InvalidAssignment(format!(
                        "segment {segment_id} is absent from layout {layout_epoch}"
                    )));
                }
                if !assigned_once.insert(*segment_id) {
                    return Err(M1FakeError::InvalidAssignment(format!(
                        "segment {segment_id} is owned by more than one member"
                    )));
                }
            }
        }
        if layout_epoch == self.current_layout.epoch && &assigned_once != assignment_segments {
            return Err(M1FakeError::InvalidAssignment(
                "a complete plan must assign every legal M1 segment exactly once".to_owned(),
            ));
        }
        Ok(())
    }

    fn retain_assignment_context(
        &mut self,
        group: &GroupKey,
        layout_epoch: u64,
        plan: &[FullAssignment],
        assignment_segments: &BTreeSet<u64>,
    ) {
        let context_names: BTreeSet<String> = plan
            .iter()
            .filter_map(|entry| {
                self.memberships
                    .get(&entry.member)
                    .filter(|membership| membership.registered)
            })
            .map(|membership| membership.consumer_name.clone())
            .collect();
        self.assignment_contexts
            .entry((group.clone(), layout_epoch))
            .and_modify(|context| {
                context
                    .legal_segments
                    .extend(assignment_segments.iter().copied());
                context.consumer_names.extend(context_names.iter().cloned());
            })
            .or_insert(AssignmentContext {
                legal_segments: assignment_segments.clone(),
                consumer_names: context_names,
            });
    }

    fn install_assignment_plan(
        &mut self,
        group: &GroupKey,
        layout_epoch: u64,
        plan: &[FullAssignment],
    ) -> Result<(), M1FakeError> {
        self.segment_owners
            .retain(|(candidate, _), _| candidate != group);
        for entry in plan {
            let assigned: BTreeSet<_> = entry.segments.iter().copied().collect();
            let membership = self
                .memberships
                .get_mut(&entry.member)
                .ok_or(M1FakeError::UnknownMember(entry.member))?;
            let registered = membership.registered;
            membership.assignment_epoch = layout_epoch;
            membership.assigned.clone_from(&assigned);
            for segment_id in assigned {
                self.segment_owners
                    .insert((group.clone(), segment_id), entry.member);
                if registered {
                    self.ownership_history
                        .insert((group.clone(), segment_id), entry.member);
                }
            }
        }
        Ok(())
    }

    fn queue_historical_assignment_plan(
        &mut self,
        group: &GroupKey,
        layout_epoch: u64,
        plan: &[FullAssignment],
    ) -> Result<(), M1FakeError> {
        let assignments: Vec<_> = plan
            .iter()
            .map(|entry| {
                let segments = entry.segments.iter().copied().collect();
                self.assignment_pb(group, layout_epoch, &segments)
                    .map(|assignment| (entry.member, assignment))
            })
            .collect::<Result<_, _>>()?;
        for (member, assignment) in assignments {
            self.queue_assignment_frame(member, assignment)?;
        }
        Ok(())
    }

    /// Re-send a member's current complete assignment without changing
    /// authoritative ownership.
    pub fn resend_assignment(&mut self, member: MemberId) -> Result<(), M1FakeError> {
        self.queue_assignment_update(member)
    }

    /// Push an older complete assignment without mutating authoritative
    /// ownership. Historical segment ids are accepted, but duplicates are not.
    pub fn push_stale_assignment(
        &mut self,
        member: MemberId,
        layout_epoch: u64,
        segments: impl IntoIterator<Item = u64>,
    ) -> Result<(), M1FakeError> {
        let membership = self
            .memberships
            .get(&member)
            .ok_or(M1FakeError::UnknownMember(member))?;
        if layout_epoch >= membership.assignment_epoch {
            return Err(M1FakeError::InvalidAssignment(format!(
                "epoch {layout_epoch} is not stale relative to {}",
                membership.assignment_epoch
            )));
        }
        let segments: Vec<_> = segments.into_iter().collect();
        let unique: BTreeSet<_> = segments.iter().copied().collect();
        if unique.len() != segments.len() {
            return Err(M1FakeError::InvalidAssignment(
                "a stale full assignment repeats a segment".to_owned(),
            ));
        }
        for segment_id in &unique {
            if !self.segment_catalog.contains_key(segment_id) {
                return Err(M1FakeError::UnknownSegment(*segment_id));
            }
        }
        let assignment = self.assignment_pb(&membership.group, layout_epoch, &unique)?;
        self.queue_assignment_frame(member, assignment)
    }

    /// Classify a child segment's parent ownership without reproducing client
    /// barrier transitions.
    pub fn ancestry_proof(
        &self,
        child_member: MemberId,
        child_segment_id: u64,
    ) -> Result<AncestryProof, M1FakeError> {
        let membership = self
            .memberships
            .get(&child_member)
            .ok_or(M1FakeError::UnknownMember(child_member))?;
        if !membership.assigned.contains(&child_segment_id) {
            return Err(M1FakeError::InvalidAssignment(format!(
                "member {child_member:?} does not own child {child_segment_id}"
            )));
        }
        let child = self
            .segment_catalog
            .get(&child_segment_id)
            .ok_or(M1FakeError::UnknownSegment(child_segment_id))?;
        if child.parent_ids.is_empty() {
            return Err(M1FakeError::InvalidLayout(format!(
                "segment {child_segment_id} has no parent edge"
            )));
        }
        let mut parent_members = BTreeSet::new();
        let mut missing = Vec::new();
        for parent_id in &child.parent_ids {
            if let Some(owner) = self
                .ownership_history
                .get(&(membership.group.clone(), *parent_id))
            {
                parent_members.insert(*owner);
            } else {
                missing.push(*parent_id);
            }
        }
        if !missing.is_empty() {
            return Ok(AncestryProof::Unknown {
                child_member,
                missing_parent_ids: missing,
            });
        }
        if parent_members.len() == 1 && parent_members.contains(&child_member) {
            Ok(AncestryProof::LocallyProvable {
                member: child_member,
                parent_ids: child.parent_ids.clone(),
            })
        } else {
            Ok(AncestryProof::CrossMemberUnprovable {
                child_member,
                parent_members: parent_members.into_iter().collect(),
                parent_ids: child.parent_ids.clone(),
            })
        }
    }

    /// Evaluate strict local drain eligibility for one assigned segment.
    pub fn drain_eligibility(
        &self,
        member: MemberId,
        segment_id: u64,
    ) -> Result<DrainEligibility, M1FakeError> {
        let membership = self
            .memberships
            .get(&member)
            .ok_or(M1FakeError::UnknownMember(member))?;
        if !membership.assigned.contains(&segment_id) {
            return Err(M1FakeError::InvalidAssignment(format!(
                "member {member:?} does not own segment {segment_id}"
            )));
        }
        let ancestors = transitive_ancestors(&self.current_layout.segments, segment_id)?;
        let mut unknown = Vec::new();
        let mut cross_member = Vec::new();
        let mut incomplete = Vec::new();
        for ancestor_id in ancestors {
            match self
                .ownership_history
                .get(&(membership.group.clone(), ancestor_id))
            {
                None => unknown.push(ancestor_id),
                Some(owner) if *owner != member => cross_member.push(ancestor_id),
                Some(_) => {
                    if !self
                        .completed_segments
                        .contains(&(membership.group.clone(), ancestor_id))
                    {
                        incomplete.push(ancestor_id);
                    }
                }
            }
        }
        if !unknown.is_empty() {
            Ok(DrainEligibility::UnknownAncestry {
                segment_ids: unknown,
            })
        } else if !cross_member.is_empty() {
            Ok(DrainEligibility::CrossMemberUnprovable {
                segment_ids: cross_member,
            })
        } else if !incomplete.is_empty() {
            Ok(DrainEligibility::ParentBlocked {
                segment_ids: incomplete,
            })
        } else {
            Ok(DrainEligibility::Eligible)
        }
    }

    /// Whether this subscription group has completed one segment's terminal
    /// drain under the fake's independent ledger/ack accounting.
    #[must_use]
    pub fn segment_is_complete(&self, subscription: &str, segment_id: u64) -> bool {
        self.completed_segments.contains(&(
            GroupKey {
                topic: self.topic.clone(),
                subscription: subscription.to_owned(),
            },
            segment_id,
        ))
    }

    /// Durable cursor for one canonical segment subscription.
    #[must_use]
    pub fn durable_cursor(&self, subscription: &str, segment_id: u64) -> Option<u64> {
        let key = ChildSubscriptionKey {
            topic: self.segment_topic(segment_id)?,
            subscription: subscription.to_owned(),
        };
        self.child_subscriptions
            .get(&key)
            .and_then(|state| state.durable_cursor)
            .and_then(|cursor| u64::try_from(cursor).ok())
    }

    /// Outstanding FLOW permits held by active children for one segment.
    #[must_use]
    pub fn segment_permits(&self, subscription: &str, segment_id: u64) -> u64 {
        self.child_consumers
            .values()
            .filter(|consumer| {
                consumer.segment_id == segment_id && consumer.group.subscription == subscription
            })
            .map(|consumer| u64::from(consumer.permits))
            .sum()
    }

    /// Controller member owning the active ordinary child for one segment.
    #[must_use]
    pub fn active_child_owner(&self, subscription: &str, segment_id: u64) -> Option<MemberId> {
        self.child_consumers.values().find_map(|consumer| {
            (consumer.segment_id == segment_id && consumer.group.subscription == subscription)
                .then_some(consumer.controller_member)
        })
    }

    /// Authoritative controller assignment owner for one segment.
    #[must_use]
    pub fn assigned_owner(&self, subscription: &str, segment_id: u64) -> Option<MemberId> {
        self.segment_owners
            .get(&(
                GroupKey {
                    topic: self.topic.clone(),
                    subscription: subscription.to_owned(),
                },
                segment_id,
            ))
            .copied()
    }

    /// Snapshot one generated transaction without exposing mutable fake state.
    #[must_use]
    pub fn transaction_observation(
        &self,
        txn_id: magnetar_proto::TxnId,
    ) -> Option<TransactionObservation> {
        self.transactions
            .get(&txn_id)
            .map(|transaction| TransactionObservation {
                id: txn_id,
                state: transaction.state,
                registered_subscriptions: transaction
                    .registered_subscriptions
                    .iter()
                    .map(|key| (key.topic.clone(), key.subscription.clone()))
                    .collect(),
                staged_acknowledgements: transaction.staged_acknowledgements.len(),
            })
    }

    /// Append one message to an active segment ledger and dispatch only when a
    /// child consumer has explicit FLOW credit. A sealed descriptor rejects the
    /// append even before explicit end-of-topic is signalled.
    pub fn enqueue_message(
        &mut self,
        segment_id: u64,
        payload: impl Into<Bytes>,
    ) -> Result<(u64, u64), M1FakeError> {
        self.enqueue_message_with_metadata(
            segment_id,
            pb::MessageMetadata::default(),
            payload,
            Vec::new(),
        )
    }

    /// Append one message with caller-supplied Pulsar metadata and batch-mask
    /// state while retaining the same ledger, FLOW, and redelivery semantics as
    /// [`Self::enqueue_message`].
    pub fn enqueue_message_with_metadata(
        &mut self,
        segment_id: u64,
        mut metadata: pb::MessageMetadata,
        payload: impl Into<Bytes>,
        ack_set: Vec<i64>,
    ) -> Result<(u64, u64), M1FakeError> {
        let segment = self
            .current_layout
            .segments
            .get(&segment_id)
            .ok_or(M1FakeError::UnknownSegment(segment_id))?;
        if segment.state == pb::SegmentState::Sealed {
            return Err(M1FakeError::InvalidCommand {
                command: pb::base_command::Type::Message,
                reason: format!("segment {segment_id} descriptor is sealed"),
            });
        }
        if self.terminal_segments.contains(&segment_id) {
            return Err(M1FakeError::InvalidCommand {
                command: pb::base_command::Type::Message,
                reason: format!("segment {segment_id} is terminal"),
            });
        }
        let payload = payload.into();
        if payload.len() > MAX_FAKE_PAYLOAD_SIZE {
            return Err(M1FakeError::InvalidCommand {
                command: pb::base_command::Type::Message,
                reason: format!("payload exceeds fake bound {MAX_FAKE_PAYLOAD_SIZE}"),
            });
        }
        let ledger = self.ledgers.entry(segment_id).or_default();
        let entry_id = ledger.len() as u64;
        if metadata.producer_name.is_empty() {
            "magnetar-m1-fake".clone_into(&mut metadata.producer_name);
        }
        metadata.sequence_id = entry_id;
        if metadata.publish_time == 0 {
            metadata.publish_time = 1_700_000_000;
        }
        ledger.push(StoredMessage {
            payload,
            metadata,
            ack_set,
        });
        let consumers: Vec<_> = self
            .child_consumers
            .iter()
            .filter_map(|(member, consumer)| (consumer.segment_id == segment_id).then_some(*member))
            .collect();
        for member in consumers {
            self.dispatch_consumer(member)?;
        }
        Ok((segment_id, entry_id))
    }

    /// Mark a segment terminal and reject later appends. Each child receives
    /// `CommandReachedEndOfTopic` only after its retained backlog has been
    /// dispatched; a child opened after termination follows the same rule.
    pub fn terminate_segment(&mut self, segment_id: u64) -> Result<(), M1FakeError> {
        if !self.current_layout.segments.contains_key(&segment_id) {
            return Err(M1FakeError::UnknownSegment(segment_id));
        }
        self.terminal_segments.insert(segment_id);
        let consumers: Vec<_> = self
            .child_consumers
            .iter()
            .filter_map(|(member, consumer)| (consumer.segment_id == segment_id).then_some(*member))
            .collect();
        for member in consumers {
            self.maybe_emit_terminal(member)?;
        }
        Ok(())
    }

    /// Routing log in command arrival order, including rejected destinations.
    #[must_use]
    pub fn routes(&self) -> &[RouteObservation] {
        &self.routes
    }

    /// Generated broker frames in queue order, retained after socket drains.
    #[must_use]
    pub fn broker_frames(&self) -> &[BrokerFrameObservation] {
        &self.broker_frames
    }

    /// Clear routing observations without changing broker state.
    pub fn clear_routes(&mut self) {
        self.routes.clear();
    }

    /// Clear generated-frame observations without changing queued output.
    pub fn clear_broker_frames(&mut self) {
        self.broker_frames.clear();
    }

    /// Snapshot resource and permit accounting.
    #[must_use]
    pub fn resource_counts(&self) -> ResourceCounts {
        let connections = self
            .connections
            .values()
            .filter(|connection| connection.connected)
            .count();
        let controller_connections = self
            .connections
            .values()
            .filter(|connection| {
                connection.connected && connection.endpoint == Endpoint::Controller
            })
            .count();
        let pending_scalable_opens = self
            .pending
            .values()
            .filter(|operation| matches!(operation, PendingOperation::ScalableOpen { .. }))
            .count();
        let pending_child_opens = self
            .pending
            .values()
            .filter(|operation| matches!(operation, PendingOperation::SegmentOpen { .. }))
            .count();
        ResourceCounts {
            connections,
            controller_connections,
            segment_connections: connections.saturating_sub(controller_connections),
            layout_sessions: self.layout_sessions.len(),
            scalable_members: self
                .memberships
                .values()
                .filter(|membership| membership.registered)
                .count(),
            pending_scalable_opens,
            child_consumers: self.child_consumers.len(),
            pending_child_opens,
            pending_operations: self.pending.len(),
            permits: self
                .child_consumers
                .values()
                .map(|consumer| u64::from(consumer.permits))
                .sum(),
            ledger_messages: self.ledgers.values().map(Vec::len).sum(),
            unacked_messages: self
                .child_consumers
                .values()
                .map(|consumer| consumer.unacked.len())
                .sum(),
        }
    }

    /// Number of unacknowledged entries retained by one segment subscription.
    #[must_use]
    pub fn segment_unacked(&self, subscription: &str, segment_id: u64) -> usize {
        self.child_consumers
            .values()
            .filter(|consumer| {
                consumer.key.subscription == subscription && consumer.segment_id == segment_id
            })
            .map(|consumer| consumer.unacked.len())
            .sum()
    }

    fn handle_frame(&mut self, connection: ConnectionId, frame: &Frame) -> Result<(), M1FakeError> {
        let kind = pb::base_command::Type::try_from(frame.command.r#type).map_err(|_| {
            M1FakeError::InvalidCommand {
                command: pb::base_command::Type::Connect,
                reason: format!("unknown BaseCommand type {}", frame.command.r#type),
            }
        })?;
        let endpoint = self.connection_endpoint(connection)?;
        self.routes.push(RouteObservation {
            connection,
            endpoint,
            command: kind,
            resource: self.command_resource(connection, frame, kind),
        });
        let handshaken = self
            .connections
            .get(&connection)
            .ok_or(M1FakeError::UnknownConnection(connection))?
            .handshaken;
        if !handshaken && kind != pb::base_command::Type::Connect {
            return Err(M1FakeError::HandshakeRequired {
                connection,
                command: kind,
            });
        }
        match kind {
            pb::base_command::Type::Connect => self.handle_connect(connection, frame),
            pb::base_command::Type::Ping => self.handle_ping(connection, frame),
            pb::base_command::Type::ScalableTopicLookup => {
                self.handle_scalable_lookup(connection, frame)
            }
            pb::base_command::Type::ScalableTopicClose => {
                self.handle_scalable_close(connection, frame)
            }
            pb::base_command::Type::ScalableTopicSubscribe => {
                self.handle_scalable_subscribe(connection, frame)
            }
            pb::base_command::Type::Lookup => self.handle_lookup(connection, frame),
            pb::base_command::Type::TcClientConnectRequest => {
                self.handle_tc_client_connect(connection, frame)
            }
            pb::base_command::Type::NewTxn => self.handle_new_transaction(connection, frame),
            pb::base_command::Type::AddSubscriptionToTxn => {
                self.handle_add_subscription_to_transaction(connection, frame)
            }
            pb::base_command::Type::EndTxn => self.handle_end_transaction(connection, frame),
            pb::base_command::Type::Subscribe => self.handle_segment_subscribe(connection, frame),
            pb::base_command::Type::GetSchema => self.handle_get_schema(connection, frame),
            pb::base_command::Type::Flow => self.handle_flow(connection, frame),
            pb::base_command::Type::Ack => self.handle_ack(connection, frame),
            pb::base_command::Type::RedeliverUnacknowledgedMessages => {
                self.handle_redeliver(connection, frame)
            }
            pb::base_command::Type::Seek => self.handle_seek(connection, frame),
            pb::base_command::Type::CloseConsumer => self.handle_close(connection, frame),
            other => Err(M1FakeError::UnsupportedCommand(other)),
        }
    }

    fn handle_connect(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        let kind = pb::base_command::Type::Connect;
        if frame.payload.is_some() {
            return Err(invalid(kind, "CONNECT must not carry a payload"));
        }
        let connect = frame
            .command
            .connect
            .as_ref()
            .ok_or_else(|| invalid(kind, "missing generated CommandConnect body"))?;
        let (endpoint, transport, handshaken) = self
            .connections
            .get(&connection)
            .map(|state| (state.endpoint, state.transport, state.handshaken))
            .ok_or(M1FakeError::UnknownConnection(connection))?;
        if handshaken {
            return Err(invalid(kind, "connection already completed CONNECT"));
        }
        if endpoint == Endpoint::Controller
            && !connect
                .feature_flags
                .as_ref()
                .and_then(|flags| flags.supports_scalable_topics)
                .unwrap_or(false)
        {
            return Err(invalid(
                kind,
                "controller CONNECT must advertise supports_scalable_topics",
            ));
        }
        if connect.proxy_to_broker_url.is_some() {
            return Err(invalid(
                kind,
                "the direct fake endpoints do not silently proxy CONNECT",
            ));
        }
        if self.auth_validator.as_ref().is_some_and(|validator| {
            !validator(AuthAttempt {
                endpoint,
                transport,
                method: connect.auth_method_name.as_deref(),
                data: connect.auth_data.as_deref(),
            })
        }) {
            return Err(M1FakeError::AuthenticationRejected { endpoint });
        }
        self.connections
            .get_mut(&connection)
            .ok_or(M1FakeError::UnknownConnection(connection))?
            .handshaken = true;
        let response = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-stateful-m1-fake".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(MAX_FRAME_SIZE as i32),
                feature_flags: Some(pb::FeatureFlags {
                    supports_scalable_topics: Some(true),
                    ..pb::FeatureFlags::default()
                }),
            }),
            ..Default::default()
        };
        self.queue_command(connection, &response)
    }

    fn handle_ping(&mut self, connection: ConnectionId, frame: &Frame) -> Result<(), M1FakeError> {
        if frame.command.ping.is_none() || frame.payload.is_some() {
            return Err(invalid(
                pb::base_command::Type::Ping,
                "missing CommandPing body or unexpected payload",
            ));
        }
        let response = pb::BaseCommand {
            r#type: pb::base_command::Type::Pong as i32,
            pong: Some(pb::CommandPong {}),
            ..Default::default()
        };
        self.queue_command(connection, &response)
    }

    fn handle_scalable_lookup(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        self.require_endpoint(
            connection,
            Endpoint::Controller,
            pb::base_command::Type::ScalableTopicLookup,
        )?;
        let lookup = frame
            .command
            .scalable_topic_lookup
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    pb::base_command::Type::ScalableTopicLookup,
                    "missing generated CommandScalableTopicLookup body",
                )
            })?;
        if lookup.topic != self.topic {
            let command = pb::BaseCommand {
                r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
                scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
                    session_id: lookup.session_id,
                    dag: None,
                    error: Some(pb::ServerError::TopicNotFound as i32),
                    message: Some("fake cluster does not serve that scalable topic".to_owned()),
                    resolved_topic_name: None,
                }),
                ..Default::default()
            };
            return self.queue_command(connection, &command);
        }
        let key = (connection, lookup.session_id);
        if self.layout_sessions.contains(&key) {
            return Err(invalid(
                pb::base_command::Type::ScalableTopicLookup,
                format!("duplicate layout session {}", lookup.session_id),
            ));
        }
        self.layout_sessions.insert(key);
        let snapshot = self.current_layout.clone();
        self.queue_layout(connection, lookup.session_id, &snapshot)?;
        // Pulsar 5.0.0-M1 answers the lookup, then re-sends the same baseline on
        // the watch it just opened. Keep this behavior load-bearing in the fake.
        self.queue_layout(connection, lookup.session_id, &snapshot)
    }

    fn handle_scalable_close(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        self.require_endpoint(
            connection,
            Endpoint::Controller,
            pb::base_command::Type::ScalableTopicClose,
        )?;
        let close = frame.command.scalable_topic_close.as_ref().ok_or_else(|| {
            invalid(
                pb::base_command::Type::ScalableTopicClose,
                "missing generated CommandScalableTopicClose body",
            )
        })?;
        // A reconnecting client may close a lookup whose response was fenced
        // before the local session became authoritative. M1 has no close
        // response, so this cleanup is idempotent.
        self.layout_sessions.remove(&(connection, close.session_id));
        // M1 has no pooled consumer-unregister command. Closing the layout watch
        // deliberately leaves controller memberships observable until physical
        // disconnect.
        Ok(())
    }

    fn handle_scalable_subscribe(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        let endpoint = self.require_endpoint(
            connection,
            Endpoint::Controller,
            pb::base_command::Type::ScalableTopicSubscribe,
        )?;
        let subscribe = frame
            .command
            .scalable_topic_subscribe
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    pb::base_command::Type::ScalableTopicSubscribe,
                    "missing generated CommandScalableTopicSubscribe body",
                )
            })?;
        if subscribe.topic != self.topic {
            return self.queue_scalable_subscribe_failure(
                connection,
                subscribe.request_id,
                &BrokerFailure::new(pb::ServerError::TopicNotFound, "unknown scalable topic"),
            );
        }
        let consumer_type =
            pb::ScalableConsumerType::try_from(subscribe.consumer_type).map_err(|_| {
                invalid(
                    pb::base_command::Type::ScalableTopicSubscribe,
                    "unknown scalable consumer type",
                )
            })?;
        if consumer_type != pb::ScalableConsumerType::Stream {
            return Err(invalid(
                pb::base_command::Type::ScalableTopicSubscribe,
                "the first-wave fake accepts Stream consumers only",
            ));
        }
        let member = MemberId::new(connection, subscribe.consumer_id);
        let group = GroupKey {
            topic: subscribe.topic.clone(),
            subscription: subscribe.subscription.clone(),
        };
        let duplicate = self.memberships.contains_key(&member)
            || self.memberships.iter().any(|(candidate, membership)| {
                *candidate != member
                    && membership.group == group
                    && membership.consumer_name == subscribe.consumer_name
            });
        if duplicate {
            return self.queue_scalable_subscribe_failure(
                connection,
                subscribe.request_id,
                &BrokerFailure::new(pb::ServerError::ConsumerBusy, "scalable member is busy"),
            );
        }

        match self.take_behavior(endpoint, OperationKind::ScalableOpen) {
            Some(ScriptedBehavior::Fail(failure)) => {
                self.queue_scalable_subscribe_failure(connection, subscribe.request_id, &failure)
            }
            behavior => {
                self.create_membership(member, group, subscribe.consumer_name.clone());
                if matches!(behavior, Some(ScriptedBehavior::Delay)) {
                    let id = self.allocate_pending_id();
                    let info = PendingOperationInfo {
                        id,
                        connection,
                        endpoint,
                        kind: OperationKind::ScalableOpen,
                        request_id: Some(subscribe.request_id),
                    };
                    self.pending
                        .insert(id, PendingOperation::ScalableOpen { info, member });
                    Ok(())
                } else {
                    self.register_membership(member)?;
                    self.queue_scalable_subscribe_response(member, subscribe.request_id, None)
                }
            }
        }
    }

    fn handle_lookup(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        self.require_endpoint(
            connection,
            Endpoint::Controller,
            pb::base_command::Type::Lookup,
        )?;
        let lookup = frame.command.lookup_topic.as_ref().ok_or_else(|| {
            invalid(
                pb::base_command::Type::Lookup,
                "missing generated CommandLookupTopic body",
            )
        })?;
        let endpoint = if lookup.topic == TRANSACTION_COORDINATOR_TOPIC {
            Some(Endpoint::Controller)
        } else {
            self.segment_id_for_topic(&lookup.topic)
                .and_then(|id| self.segment_endpoint(id))
        };
        let (response, error, message, urls) = if let Some(endpoint) = endpoint {
            let authorities = self.authorities.get(&endpoint).cloned();
            (
                pb::command_lookup_topic_response::LookupType::Connect,
                None,
                None,
                authorities,
            )
        } else {
            (
                pb::command_lookup_topic_response::LookupType::Failed,
                Some(pb::ServerError::TopicNotFound as i32),
                Some("fake segment topic is not active".to_owned()),
                None,
            )
        };
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::LookupResponse as i32,
            lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                broker_service_url: urls.as_ref().map(|urls| urls.plaintext.clone()),
                broker_service_url_tls: urls.map(|urls| urls.tls),
                response: Some(response as i32),
                request_id: lookup.request_id,
                authoritative: Some(true),
                error,
                message,
                proxy_through_service_url: Some(false),
            }),
            ..Default::default()
        };
        self.queue_command(connection, &command)
    }

    fn handle_get_schema(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        let command = frame.command.get_schema.as_ref().ok_or_else(|| {
            invalid(
                pb::base_command::Type::GetSchema,
                "missing generated CommandGetSchema body",
            )
        })?;
        let segment_id = self.segment_id_for_topic(&command.topic).ok_or_else(|| {
            invalid(
                pb::base_command::Type::GetSchema,
                format!("unknown segment topic {}", command.topic),
            )
        })?;
        let endpoint = self.segment_endpoint(segment_id).ok_or_else(|| {
            invalid(
                pb::base_command::Type::GetSchema,
                format!("segment {segment_id} has no serving placement"),
            )
        })?;
        self.require_endpoint(connection, endpoint, pb::base_command::Type::GetSchema)?;
        match self.take_behavior(endpoint, OperationKind::GetSchema) {
            Some(ScriptedBehavior::Fail(failure)) => self.queue_get_schema_response(
                connection,
                command.request_id,
                command.topic.clone(),
                command.schema_version.clone(),
                Some(&failure),
            ),
            Some(ScriptedBehavior::Delay) => {
                let id = self.allocate_pending_id();
                let info = PendingOperationInfo {
                    id,
                    endpoint,
                    kind: OperationKind::GetSchema,
                    connection,
                    request_id: Some(command.request_id),
                };
                self.pending.insert(
                    id,
                    PendingOperation::GetSchema {
                        info,
                        request_id: command.request_id,
                        topic: command.topic.clone(),
                        schema_version: command.schema_version.clone(),
                    },
                );
                Ok(())
            }
            None => self.queue_get_schema_response(
                connection,
                command.request_id,
                command.topic.clone(),
                command.schema_version.clone(),
                None,
            ),
        }
    }

    fn handle_tc_client_connect(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        self.require_endpoint(
            connection,
            Endpoint::Controller,
            pb::base_command::Type::TcClientConnectRequest,
        )?;
        let request = frame
            .command
            .tc_client_connect_request
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    pb::base_command::Type::TcClientConnectRequest,
                    "missing generated CommandTcClientConnectRequest body",
                )
            })?;
        if frame.payload.is_some() || request.tc_id != 0 || request.scalable == Some(true) {
            return Err(invalid(
                pb::base_command::Type::TcClientConnectRequest,
                "legacy transaction coordinator handshake must target tc_id 0 without payload",
            ));
        }
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::TcClientConnectResponse as i32,
            tc_client_connect_response: Some(pb::CommandTcClientConnectResponse {
                request_id: request.request_id,
                error: None,
                message: None,
            }),
            ..Default::default()
        };
        self.queue_command(connection, &command)
    }

    fn handle_new_transaction(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        self.require_endpoint(
            connection,
            Endpoint::Controller,
            pb::base_command::Type::NewTxn,
        )?;
        let request = frame.command.new_txn.as_ref().ok_or_else(|| {
            invalid(
                pb::base_command::Type::NewTxn,
                "missing generated CommandNewTxn body",
            )
        })?;
        if frame.payload.is_some()
            || request.tc_id != Some(0)
            || request.txn_ttl_millis == Some(0)
            || request.txn_ttl_millis.is_none()
            || request.scalable == Some(true)
        {
            return Err(invalid(
                pb::base_command::Type::NewTxn,
                "new transaction requires legacy tc_id 0 and a positive TTL",
            ));
        }
        let txn_id = magnetar_proto::TxnId::new(0, self.next_transaction_sequence);
        self.next_transaction_sequence = self
            .next_transaction_sequence
            .checked_add(1)
            .ok_or_else(|| invalid(pb::base_command::Type::NewTxn, "transaction id exhausted"))?;
        self.transactions.insert(
            txn_id,
            FakeTransaction {
                state: FakeTransactionState::Open,
                registered_subscriptions: BTreeSet::new(),
                staged_acknowledgements: Vec::new(),
            },
        );
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::NewTxnResponse as i32,
            new_txn_response: Some(pb::CommandNewTxnResponse {
                request_id: request.request_id,
                txnid_least_bits: Some(txn_id.least_sig_bits),
                txnid_most_bits: Some(txn_id.most_sig_bits),
                error: None,
                message: None,
            }),
            ..Default::default()
        };
        self.queue_command(connection, &command)
    }

    fn handle_add_subscription_to_transaction(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        let endpoint = self.require_endpoint(
            connection,
            Endpoint::Controller,
            pb::base_command::Type::AddSubscriptionToTxn,
        )?;
        let request = frame
            .command
            .add_subscription_to_txn
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    pb::base_command::Type::AddSubscriptionToTxn,
                    "missing generated CommandAddSubscriptionToTxn body",
                )
            })?;
        if frame.payload.is_some()
            || request.scalable == Some(true)
            || request.subscription.len() != 1
        {
            return Err(invalid(
                pb::base_command::Type::AddSubscriptionToTxn,
                "legacy registration must name exactly one subscription without payload",
            ));
        }
        let txn_id = transaction_id(
            pb::base_command::Type::AddSubscriptionToTxn,
            request.txnid_most_bits,
            request.txnid_least_bits,
        )?;
        self.require_open_transaction(txn_id, pb::base_command::Type::AddSubscriptionToTxn)?;
        let registration = &request.subscription[0];
        if self.segment_id_for_topic(&registration.topic).is_none() {
            return Err(invalid(
                pb::base_command::Type::AddSubscriptionToTxn,
                "transaction registration must name a canonical fake segment topic",
            ));
        }
        let key = ChildSubscriptionKey {
            topic: registration.topic.clone(),
            subscription: registration.subscription.clone(),
        };
        if !self.child_subscriptions.contains_key(&key) {
            return Err(invalid(
                pb::base_command::Type::AddSubscriptionToTxn,
                "transaction registration has no matching child subscription",
            ));
        }
        match self.take_behavior(endpoint, OperationKind::TransactionRegistration) {
            Some(ScriptedBehavior::Fail(failure)) => self.queue_add_subscription_response(
                connection,
                request.request_id,
                txn_id,
                Some(&failure),
            ),
            Some(ScriptedBehavior::Delay) => {
                let id = self.allocate_pending_id();
                let info = PendingOperationInfo {
                    id,
                    connection,
                    endpoint,
                    kind: OperationKind::TransactionRegistration,
                    request_id: Some(request.request_id),
                };
                self.pending.insert(
                    id,
                    PendingOperation::TransactionRegistration {
                        info,
                        txn_id,
                        key,
                        request_id: request.request_id,
                    },
                );
                Ok(())
            }
            None => {
                self.register_transaction_subscription(txn_id, &key)?;
                self.queue_add_subscription_response(connection, request.request_id, txn_id, None)
            }
        }
    }

    fn handle_end_transaction(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        let command_type = pb::base_command::Type::EndTxn;
        let endpoint = self.require_endpoint(connection, Endpoint::Controller, command_type)?;
        let request = frame
            .command
            .end_txn
            .as_ref()
            .ok_or_else(|| invalid(command_type, "missing generated CommandEndTxn body"))?;
        if frame.payload.is_some() || request.scalable == Some(true) {
            return Err(invalid(
                command_type,
                "legacy end transaction must not carry scalable mode or payload",
            ));
        }
        let txn_id = transaction_id(
            command_type,
            request.txnid_most_bits,
            request.txnid_least_bits,
        )?;
        self.require_open_transaction(txn_id, command_type)?;
        if self.pending.values().any(|operation| {
            matches!(operation, PendingOperation::TransactionRegistration { txn_id: pending, .. } if *pending == txn_id)
                || matches!(operation, PendingOperation::Ack { txn_id: Some(pending), .. } if *pending == txn_id)
        }) {
            return Err(invalid(
                command_type,
                "transaction still has admitted work in flight",
            ));
        }
        let action = request
            .txn_action
            .and_then(|value| pb::TxnAction::try_from(value).ok())
            .ok_or_else(|| invalid(command_type, "missing transaction action"))?;
        match self.take_behavior(endpoint, OperationKind::EndTransaction) {
            Some(ScriptedBehavior::Fail(failure)) => self.queue_end_transaction_response(
                connection,
                request.request_id,
                txn_id,
                Some(&failure),
            ),
            Some(ScriptedBehavior::Delay) => {
                let id = self.allocate_pending_id();
                let info = PendingOperationInfo {
                    id,
                    connection,
                    endpoint,
                    kind: OperationKind::EndTransaction,
                    request_id: Some(request.request_id),
                };
                self.pending.insert(
                    id,
                    PendingOperation::EndTransaction {
                        info,
                        txn_id,
                        request_id: request.request_id,
                        action,
                    },
                );
                Ok(())
            }
            None => self.complete_end_transaction(connection, request.request_id, txn_id, action),
        }
    }

    fn complete_end_transaction(
        &mut self,
        connection: ConnectionId,
        request_id: u64,
        txn_id: magnetar_proto::TxnId,
        action: pb::TxnAction,
    ) -> Result<(), M1FakeError> {
        let command_type = pb::base_command::Type::EndTxn;
        self.require_open_transaction(txn_id, command_type)?;
        let staged = self
            .transactions
            .get(&txn_id)
            .map(|transaction| transaction.staged_acknowledgements.clone())
            .ok_or_else(|| invalid(command_type, "unknown transaction"))?;
        for acknowledgement in &staged {
            if !self.child_fence_matches(acknowledgement.member, &acknowledgement.fence) {
                return Err(invalid(
                    command_type,
                    "staged acknowledgement belongs to a stale child incarnation",
                ));
            }
        }
        match action {
            pb::TxnAction::Commit => {
                for acknowledgement in &staged {
                    self.apply_ack(
                        acknowledgement.member,
                        acknowledgement.ack_type,
                        &acknowledgement.indices,
                    );
                    self.maybe_emit_terminal(acknowledgement.member)?;
                    self.refresh_segment_completion(acknowledgement.member);
                }
            }
            pb::TxnAction::Abort => {
                for acknowledgement in &staged {
                    let consumer = self
                        .child_consumers
                        .get_mut(&acknowledgement.member)
                        .ok_or_else(|| invalid(command_type, "unknown staged child"))?;
                    for index in &acknowledgement.indices {
                        if !consumer.redeliver.contains(index) {
                            consumer.redeliver.push_back(*index);
                        }
                    }
                }
            }
        }
        let transaction = self
            .transactions
            .get_mut(&txn_id)
            .ok_or_else(|| invalid(command_type, "unknown transaction"))?;
        transaction.state = match action {
            pb::TxnAction::Commit => FakeTransactionState::Committed,
            pb::TxnAction::Abort => FakeTransactionState::Aborted,
        };
        transaction.staged_acknowledgements.clear();
        self.queue_end_transaction_response(connection, request_id, txn_id, None)?;
        if action == pb::TxnAction::Abort {
            let members: BTreeSet<_> = staged
                .iter()
                .map(|acknowledgement| acknowledgement.member)
                .collect();
            for member in members {
                self.dispatch_consumer(member)?;
            }
        }
        Ok(())
    }

    fn handle_segment_subscribe(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        let subscribe = frame.command.subscribe.as_ref().ok_or_else(|| {
            invalid(
                pb::base_command::Type::Subscribe,
                "missing generated CommandSubscribe body",
            )
        })?;
        if self.child_open_needs_ownership_retry(connection, subscribe)? {
            return self.queue_error(
                connection,
                subscribe.request_id,
                &BrokerFailure::new(
                    pb::ServerError::ConsumerBusy,
                    "segment ownership moved to another scalable member",
                ),
            );
        }
        let validated = self.validate_child_open(connection, subscribe)?;
        if self
            .child_subscriptions
            .get(&validated.key)
            .and_then(|state| state.owner)
            .is_some()
        {
            return self.queue_error(
                connection,
                subscribe.request_id,
                &BrokerFailure::new(pb::ServerError::ConsumerBusy, "exclusive consumer is busy"),
            );
        }
        let cursor = self.initial_child_cursor(&validated.key, validated.segment_id, subscribe);
        let generation = {
            let subscription = self
                .child_subscriptions
                .entry(validated.key.clone())
                .or_default();
            let generation = subscription.next_generation;
            subscription.next_generation = subscription.next_generation.saturating_add(1);
            generation
        };
        let activation = ChildActivation {
            member: validated.member,
            segment_id: validated.segment_id,
            group: validated.group,
            controller_member: validated.controller_member,
            serving_endpoint: validated.serving_endpoint,
            generation,
            key: validated.key,
            cursor,
        };
        match self.take_behavior(activation.serving_endpoint, OperationKind::SegmentOpen) {
            Some(ScriptedBehavior::Fail(failure)) => {
                self.queue_error(connection, subscribe.request_id, &failure)
            }
            Some(ScriptedBehavior::Delay) => {
                let id = self.allocate_pending_id();
                let subscription = self
                    .child_subscriptions
                    .entry(activation.key.clone())
                    .or_default();
                subscription.owner = Some(ChildOwner::Pending(id));
                let info = PendingOperationInfo {
                    id,
                    connection,
                    endpoint: activation.serving_endpoint,
                    kind: OperationKind::SegmentOpen,
                    request_id: Some(subscribe.request_id),
                };
                self.pending.insert(
                    id,
                    PendingOperation::SegmentOpen {
                        info,
                        command: Box::new(subscribe.clone()),
                        activation,
                    },
                );
                Ok(())
            }
            None => {
                let member = activation.member;
                self.activate_child_consumer(activation);
                self.queue_success(connection, subscribe.request_id)?;
                self.maybe_emit_terminal(member)
            }
        }
    }

    fn child_open_needs_ownership_retry(
        &self,
        connection: ConnectionId,
        subscribe: &pb::CommandSubscribe,
    ) -> Result<bool, M1FakeError> {
        let Some(segment_id) = self.segment_id_for_topic(&subscribe.topic) else {
            return Ok(false);
        };
        let Some(expected_endpoint) = self.segment_endpoint(segment_id) else {
            return Ok(false);
        };
        self.require_endpoint(
            connection,
            expected_endpoint,
            pb::base_command::Type::Subscribe,
        )?;
        let suffix = format!("-seg-{segment_id}");
        let Some(consumer_name) = subscribe
            .consumer_name
            .as_deref()
            .and_then(|name| name.strip_suffix(&suffix))
        else {
            return Ok(false);
        };
        let group = GroupKey {
            topic: self.topic.clone(),
            subscription: subscribe.subscription.clone(),
        };
        let member = self.memberships.iter().find_map(|(member, membership)| {
            (membership.registered
                && membership.group == group
                && membership.consumer_name == consumer_name)
                .then_some(*member)
        });
        if let Some(member) = member {
            return Ok(self.segment_owners.get(&(group, segment_id)) != Some(&member));
        }
        Ok(self
            .baselines
            .contains_key(&(group, consumer_name.to_owned())))
    }

    fn validate_child_open(
        &self,
        connection: ConnectionId,
        subscribe: &pb::CommandSubscribe,
    ) -> Result<ValidatedChildOpen, M1FakeError> {
        let segment_id = self.segment_id_for_topic(&subscribe.topic).ok_or_else(|| {
            invalid(
                pb::base_command::Type::Subscribe,
                format!("unknown segment topic {}", subscribe.topic),
            )
        })?;
        let expected = self.segment_endpoint(segment_id).ok_or_else(|| {
            invalid(
                pb::base_command::Type::Subscribe,
                format!("segment {segment_id} has no serving placement"),
            )
        })?;
        let serving_endpoint =
            self.require_endpoint(connection, expected, pb::base_command::Type::Subscribe)?;
        if pb::command_subscribe::SubType::try_from(subscribe.sub_type)
            != Ok(pb::command_subscribe::SubType::Exclusive)
        {
            return Err(invalid(
                pb::base_command::Type::Subscribe,
                "segment consumers must use Exclusive ownership",
            ));
        }
        let group = GroupKey {
            topic: self.topic.clone(),
            subscription: subscribe.subscription.clone(),
        };
        let controller_member = self
            .segment_owners
            .get(&(group.clone(), segment_id))
            .copied()
            .ok_or_else(|| {
                invalid(
                    pb::base_command::Type::Subscribe,
                    format!(
                        "segment {segment_id} is not assigned for subscription {}",
                        subscribe.subscription
                    ),
                )
            })?;
        let controller_membership = self
            .memberships
            .get(&controller_member)
            .ok_or(M1FakeError::UnknownMember(controller_member))?;
        if !controller_membership.registered
            || controller_membership.group != group
            || !controller_membership.assigned.contains(&segment_id)
        {
            return Err(invalid(
                pb::base_command::Type::Subscribe,
                "ordinary child does not match registered controller ownership",
            ));
        }
        let expected_name = format!("{}-seg-{segment_id}", controller_membership.consumer_name);
        if subscribe.consumer_name.as_deref() != Some(expected_name.as_str()) {
            return Err(invalid(
                pb::base_command::Type::Subscribe,
                format!("ordinary child name must be `{expected_name}`"),
            ));
        }
        let member = MemberId::new(connection, subscribe.consumer_id);
        if self.child_consumers.contains_key(&member)
            || self.pending.values().any(|operation| {
                matches!(operation, PendingOperation::SegmentOpen { activation, .. }
                    if activation.member == member)
            })
        {
            return Err(invalid(
                pb::base_command::Type::Subscribe,
                format!("consumer id {} is already in use", subscribe.consumer_id),
            ));
        }
        Ok(ValidatedChildOpen {
            member,
            segment_id,
            group,
            controller_member,
            serving_endpoint,
            key: ChildSubscriptionKey {
                topic: subscribe.topic.clone(),
                subscription: subscribe.subscription.clone(),
            },
        })
    }

    fn handle_flow(&mut self, connection: ConnectionId, frame: &Frame) -> Result<(), M1FakeError> {
        let flow = frame.command.flow.as_ref().ok_or_else(|| {
            invalid(
                pb::base_command::Type::Flow,
                "missing generated CommandFlow body",
            )
        })?;
        let member = MemberId::new(connection, flow.consumer_id);
        let consumer = self.child_consumers.get_mut(&member).ok_or_else(|| {
            invalid(
                pb::base_command::Type::Flow,
                format!("unknown child consumer {}", flow.consumer_id),
            )
        })?;
        let owns_segment = self
            .segment_owners
            .get(&(consumer.group.clone(), consumer.segment_id))
            == Some(&consumer.controller_member);
        if !owns_segment {
            return Err(invalid(
                pb::base_command::Type::Flow,
                "child controller no longer owns this segment",
            ));
        }
        if consumer.closing {
            return Err(invalid(
                pb::base_command::Type::Flow,
                "consumer has a delayed close in flight",
            ));
        }
        consumer.permits = consumer
            .permits
            .checked_add(flow.message_permits)
            .ok_or_else(|| invalid(pb::base_command::Type::Flow, "FLOW permit counter overflow"))?;
        self.dispatch_consumer(member)
    }

    #[allow(clippy::too_many_lines)]
    fn handle_ack(&mut self, connection: ConnectionId, frame: &Frame) -> Result<(), M1FakeError> {
        let ack = frame.command.ack.as_ref().ok_or_else(|| {
            invalid(
                pb::base_command::Type::Ack,
                "missing generated CommandAck body",
            )
        })?;
        let member = MemberId::new(connection, ack.consumer_id);
        let consumer = self.child_consumers.get(&member).ok_or_else(|| {
            invalid(
                pb::base_command::Type::Ack,
                format!("unknown child consumer {}", ack.consumer_id),
            )
        })?;
        let endpoint = consumer.serving_endpoint;
        let key = consumer.key.clone();
        let txn_id = optional_transaction_id(
            pb::base_command::Type::Ack,
            ack.txnid_most_bits,
            ack.txnid_least_bits,
        )?;
        if let Some(txn_id) = txn_id {
            self.require_open_transaction(txn_id, pb::base_command::Type::Ack)?;
            if ack.request_id.is_none() {
                return Err(invalid(
                    pb::base_command::Type::Ack,
                    "transactional ACK requires a request id",
                ));
            }
            if !self
                .transactions
                .get(&txn_id)
                .is_some_and(|transaction| transaction.registered_subscriptions.contains(&key))
            {
                return Err(invalid(
                    pb::base_command::Type::Ack,
                    "transactional ACK arrived before subscription registration",
                ));
            }
        }
        let ack_type = pb::command_ack::AckType::try_from(ack.ack_type)
            .map_err(|_| invalid(pb::base_command::Type::Ack, "unknown acknowledgement type"))?;
        if ack.message_id.is_empty() {
            return Err(invalid(
                pb::base_command::Type::Ack,
                "ACK must name at least one delivered message",
            ));
        }
        if ack_type == pb::command_ack::AckType::Cumulative && ack.message_id.len() != 1 {
            return Err(invalid(
                pb::base_command::Type::Ack,
                "cumulative ACK must carry exactly one message id",
            ));
        }
        let indices =
            Self::validate_delivered_ids(consumer, &ack.message_id, pb::base_command::Type::Ack)?;
        let fence = child_fence(member, consumer);
        match self.take_behavior(endpoint, OperationKind::Ack) {
            Some(ScriptedBehavior::Fail(failure)) => self.queue_ack_response(
                connection,
                member.consumer_id,
                ack.request_id,
                txn_id,
                Some(&failure),
            ),
            Some(ScriptedBehavior::Delay) => {
                let id = self.allocate_pending_id();
                let info = PendingOperationInfo {
                    id,
                    connection,
                    endpoint,
                    kind: OperationKind::Ack,
                    request_id: ack.request_id,
                };
                self.pending.insert(
                    id,
                    PendingOperation::Ack {
                        info,
                        member,
                        ack_type,
                        indices,
                        request_id: ack.request_id,
                        fence,
                        txn_id,
                    },
                );
                Ok(())
            }
            None => self
                .apply_or_stage_ack(member, ack_type, &indices, &fence, txn_id)
                .and_then(|()| {
                    self.queue_ack_response(
                        connection,
                        member.consumer_id,
                        ack.request_id,
                        txn_id,
                        None,
                    )
                })
                .and_then(|()| self.maybe_emit_terminal(member))
                .map(|()| self.refresh_segment_completion(member)),
        }
    }

    fn handle_redeliver(
        &mut self,
        connection: ConnectionId,
        frame: &Frame,
    ) -> Result<(), M1FakeError> {
        let redeliver = frame
            .command
            .redeliver_unacknowledged_messages
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    pb::base_command::Type::RedeliverUnacknowledgedMessages,
                    "missing generated CommandRedeliverUnacknowledgedMessages body",
                )
            })?;
        let member = MemberId::new(connection, redeliver.consumer_id);
        let consumer = self.child_consumers.get_mut(&member).ok_or_else(|| {
            invalid(
                pb::base_command::Type::RedeliverUnacknowledgedMessages,
                "unknown child consumer",
            )
        })?;
        let indices = if redeliver.message_ids.is_empty() {
            consumer.unacked.iter().copied().collect()
        } else {
            Self::validate_delivered_ids(
                consumer,
                &redeliver.message_ids,
                pb::base_command::Type::RedeliverUnacknowledgedMessages,
            )?
        };
        for index in indices {
            if !consumer.redeliver.contains(&index) {
                consumer.redeliver.push_back(index);
            }
        }
        self.dispatch_consumer(member)
    }

    fn handle_seek(&mut self, connection: ConnectionId, frame: &Frame) -> Result<(), M1FakeError> {
        let seek = frame.command.seek.as_ref().ok_or_else(|| {
            invalid(
                pb::base_command::Type::Seek,
                "missing generated CommandSeek body",
            )
        })?;
        if seek.message_publish_time.is_some() {
            return Err(invalid(
                pb::base_command::Type::Seek,
                "the focused fake accepts message-id seeks only",
            ));
        }
        let member = MemberId::new(connection, seek.consumer_id);
        let consumer = self
            .child_consumers
            .get(&member)
            .ok_or_else(|| invalid(pb::base_command::Type::Seek, "unknown child consumer"))?;
        let endpoint = consumer.serving_endpoint;
        let segment_id = consumer.segment_id;
        let closing = consumer.closing;
        if closing {
            return Err(invalid(
                pb::base_command::Type::Seek,
                "consumer has a delayed close in flight",
            ));
        }
        let target = if let Some(seek_id) = &seek.message_id {
            if seek_id.ledger_id != segment_id {
                return Err(invalid(
                    pb::base_command::Type::Seek,
                    "message id belongs to another segment",
                ));
            }
            let ledger_len = self.ledgers.get(&segment_id).map_or(0, Vec::len);
            if seek_id.entry_id > ledger_len as u64 {
                return Err(invalid(
                    pb::base_command::Type::Seek,
                    "message id is beyond the fake ledger",
                ));
            }
            // Pulsar 5.0.0-M1's ServerCnx projects seek positions from ledger,
            // entry, and ack_set; partition and batch metadata are ignored.
            seek_id.entry_id as usize
        } else {
            0
        };
        let fence = self.child_fence(member)?;
        match self.take_behavior(endpoint, OperationKind::Seek) {
            Some(ScriptedBehavior::Fail(failure)) => {
                self.queue_error(connection, seek.request_id, &failure)
            }
            Some(ScriptedBehavior::Delay) => {
                let id = self.allocate_pending_id();
                let info = PendingOperationInfo {
                    id,
                    connection,
                    endpoint,
                    kind: OperationKind::Seek,
                    request_id: Some(seek.request_id),
                };
                self.pending.insert(
                    id,
                    PendingOperation::Seek {
                        info,
                        member,
                        request_id: seek.request_id,
                        target,
                        fence,
                    },
                );
                Ok(())
            }
            None => {
                self.apply_seek(member, target)?;
                self.queue_success(connection, seek.request_id)
            }
        }
    }

    fn handle_close(&mut self, connection: ConnectionId, frame: &Frame) -> Result<(), M1FakeError> {
        let close = frame.command.close_consumer.as_ref().ok_or_else(|| {
            invalid(
                pb::base_command::Type::CloseConsumer,
                "missing generated CommandCloseConsumer body",
            )
        })?;
        let member = MemberId::new(connection, close.consumer_id);
        let Some(consumer) = self.child_consumers.get(&member) else {
            let endpoint = self.connection_endpoint(connection)?;
            if matches!(endpoint, Endpoint::Segment(_)) && self.closed_consumers.contains(&member) {
                return self.queue_success(connection, close.request_id);
            }
            return Err(invalid(
                pb::base_command::Type::CloseConsumer,
                format!("unknown child consumer {}", close.consumer_id),
            ));
        };
        let endpoint = consumer.serving_endpoint;
        let fence = child_fence(member, consumer);
        if self
            .child_consumers
            .get(&member)
            .is_some_and(|consumer| consumer.closing)
        {
            return Err(invalid(
                pb::base_command::Type::CloseConsumer,
                "consumer already has a delayed close in flight",
            ));
        }
        match self.take_behavior(endpoint, OperationKind::Close) {
            Some(ScriptedBehavior::Fail(failure)) => {
                self.queue_error(connection, close.request_id, &failure)
            }
            Some(ScriptedBehavior::Delay) => {
                if let Some(consumer) = self.child_consumers.get_mut(&member) {
                    consumer.closing = true;
                }
                let id = self.allocate_pending_id();
                let info = PendingOperationInfo {
                    id,
                    connection,
                    endpoint,
                    kind: OperationKind::Close,
                    request_id: Some(close.request_id),
                };
                self.pending.insert(
                    id,
                    PendingOperation::Close {
                        info,
                        member,
                        request_id: close.request_id,
                        fence,
                    },
                );
                Ok(())
            }
            None => {
                self.remove_child_consumer(member);
                self.queue_success(connection, close.request_id)
            }
        }
    }

    fn create_membership(&mut self, member: MemberId, group: GroupKey, consumer_name: String) {
        let assignment_segments = self.current_assignment_segment_ids(&group);
        let baseline_key = (group.clone(), consumer_name.clone());
        let baseline = self.baselines.get(&baseline_key).cloned();
        let reserved: BTreeSet<_> = self
            .baselines
            .iter()
            .filter(|(key, _)| key.0 == group && **key != baseline_key)
            .flat_map(|(_, saved)| saved.segments.iter().copied())
            .collect();
        let mut candidates = baseline
            .as_ref()
            .map_or_else(BTreeSet::new, |saved| saved.segments.clone());
        candidates.extend(assignment_segments.difference(&reserved).copied());
        let assigned: BTreeSet<_> = candidates
            .intersection(&assignment_segments)
            .filter(|segment_id| {
                !self
                    .segment_owners
                    .contains_key(&(group.clone(), **segment_id))
            })
            .copied()
            .collect();
        let assignment_epoch = baseline.map_or(self.current_layout.epoch, |saved| {
            saved.epoch.max(self.current_layout.epoch)
        });
        for segment_id in &assigned {
            self.segment_owners
                .insert((group.clone(), *segment_id), member);
        }
        self.memberships.insert(
            member,
            Membership {
                group,
                consumer_name,
                registered: false,
                assignment_epoch,
                assigned,
            },
        );
    }

    fn register_membership(&mut self, member: MemberId) -> Result<(), M1FakeError> {
        let (baseline_key, group, assigned) = {
            let membership = self
                .memberships
                .get_mut(&member)
                .ok_or(M1FakeError::UnknownMember(member))?;
            membership.registered = true;
            (
                (membership.group.clone(), membership.consumer_name.clone()),
                membership.group.clone(),
                membership.assigned.clone(),
            )
        };
        self.baselines.remove(&baseline_key);
        for segment_id in assigned {
            self.ownership_history
                .insert((group.clone(), segment_id), member);
        }
        self.record_current_assignment_context(&group);
        Ok(())
    }

    fn remove_membership(&mut self, member: MemberId, save_baseline: bool) {
        self.memberships
            .remove(&member)
            .into_iter()
            .for_each(|membership| {
                if save_baseline {
                    self.baselines.insert(
                        (membership.group.clone(), membership.consumer_name.clone()),
                        Baseline {
                            epoch: membership.assignment_epoch,
                            segments: membership.assigned.clone(),
                        },
                    );
                }
                self.segment_owners
                    .retain(|_, candidate| *candidate != member);
            });
    }

    fn initial_child_cursor(
        &self,
        key: &ChildSubscriptionKey,
        segment_id: u64,
        subscribe: &pb::CommandSubscribe,
    ) -> usize {
        if let Some(cursor) = self
            .child_subscriptions
            .get(key)
            .and_then(|state| state.durable_cursor)
        {
            return cursor;
        }
        let earliest = subscribe.initial_position
            == Some(pb::command_subscribe::InitialPosition::Earliest as i32);
        if earliest {
            0
        } else {
            self.ledgers.get(&segment_id).map_or(0, Vec::len)
        }
    }

    fn activate_child_consumer(&mut self, activation: ChildActivation) {
        self.closed_consumers.remove(&activation.member);
        let subscription = self
            .child_subscriptions
            .entry(activation.key.clone())
            .or_default();
        subscription.owner = Some(ChildOwner::Active(activation.member));
        subscription.durable_cursor = Some(
            subscription
                .durable_cursor
                .map_or(activation.cursor, |cursor| cursor.max(activation.cursor)),
        );
        self.child_consumers.insert(
            activation.member,
            ChildConsumer {
                segment_id: activation.segment_id,
                group: activation.group,
                key: activation.key,
                controller_member: activation.controller_member,
                serving_endpoint: activation.serving_endpoint,
                generation: activation.generation,
                delivery_cursor: activation.cursor,
                permits: 0,
                unacked: BTreeSet::new(),
                redeliver: VecDeque::new(),
                redelivery_counts: BTreeMap::new(),
                closing: false,
                terminal_sent: false,
            },
        );
    }

    fn remove_child_consumer(&mut self, member: MemberId) {
        let consumer = self
            .child_consumers
            .remove(&member)
            .expect("child-removal callers retain an active consumer");
        self.closed_consumers.insert(member);
        let subscription = self
            .child_subscriptions
            .get_mut(&consumer.key)
            .expect("an active child retains its subscription");
        debug_assert!(
            matches!(subscription.owner, Some(ChildOwner::Active(owner)) if owner == member)
        );
        subscription.owner = None;
    }

    fn release_pending_child_owner(
        &mut self,
        key: &ChildSubscriptionKey,
        pending_id: PendingOperationId,
    ) {
        if let Some(subscription) = self.child_subscriptions.get_mut(key)
            && matches!(subscription.owner, Some(ChildOwner::Pending(owner)) if owner == pending_id)
        {
            subscription.owner = None;
        }
    }

    fn cancel_pending(&mut self, id: PendingOperationId) {
        self.pending.remove(&id).into_iter().for_each(|operation| {
            if let PendingOperation::ScalableOpen { member, .. } = &operation {
                self.remove_membership(*member, false);
            }
            if let PendingOperation::SegmentOpen { activation, .. } = &operation {
                self.release_pending_child_owner(&activation.key, id);
            }
            if let PendingOperation::Close { member, .. } = &operation
                && let Some(consumer) = self.child_consumers.get_mut(member)
            {
                consumer.closing = false;
            }
        });
    }

    fn validate_layout(
        &self,
        epoch: u64,
        segments: Vec<M1Segment>,
    ) -> Result<BTreeMap<u64, M1Segment>, M1FakeError> {
        if segments.is_empty() || segments.len() > MAX_DAG_SEGMENTS {
            return Err(M1FakeError::InvalidLayout(format!(
                "a complete layout must contain 1..={MAX_DAG_SEGMENTS} segments"
            )));
        }
        let mut by_id = BTreeMap::new();
        for segment in segments {
            if by_id.insert(segment.id, segment).is_some() {
                return Err(M1FakeError::InvalidLayout(
                    "a complete layout repeats a segment id".to_owned(),
                ));
            }
        }
        self.validate_layout_identity(epoch, &by_id)?;
        Self::validate_layout_edges(epoch, &by_id)?;
        validate_acyclic(&by_id)?;
        validate_active_leaf_coverage(&by_id)?;
        validate_transition_ranges(&by_id)?;
        self.validate_layout_wire_size(epoch, &by_id)?;
        Ok(by_id)
    }

    fn validate_layout_identity(
        &self,
        epoch: u64,
        segments: &BTreeMap<u64, M1Segment>,
    ) -> Result<(), M1FakeError> {
        self.validate_layout_history_rewrite(segments)?;
        for segment in segments.values() {
            self.validate_segment_descriptor(epoch, segment)?;
        }
        Ok(())
    }

    fn validate_layout_history_rewrite(
        &self,
        segments: &BTreeMap<u64, M1Segment>,
    ) -> Result<(), M1FakeError> {
        let durable_groups: BTreeSet<GroupKey> = self
            .memberships
            .values()
            .filter(|membership| membership.registered)
            .map(|membership| membership.group.clone())
            .chain(self.baselines.keys().map(|(group, _)| group.clone()))
            .chain(
                self.ownership_history
                    .keys()
                    .map(|(group, _)| group.clone()),
            )
            .collect();
        let removed: BTreeSet<_> = self
            .current_layout
            .segments
            .keys()
            .filter(|segment_id| !segments.contains_key(segment_id))
            .copied()
            .collect();
        for segment_id in &removed {
            let previous = self
                .current_layout
                .segments
                .get(segment_id)
                .ok_or(M1FakeError::UnknownSegment(*segment_id))?;
            if previous.state != pb::SegmentState::Sealed {
                return Err(M1FakeError::InvalidLayout(format!(
                    "complete snapshot dropped active segment {segment_id}"
                )));
            }
            if let Some(group) = durable_groups.iter().find(|group| {
                !self
                    .completed_segments
                    .contains(&((*group).clone(), *segment_id))
            }) {
                return Err(M1FakeError::InvalidLayout(format!(
                    "complete snapshot garbage-collected undrained segment {segment_id} for {}/{}",
                    group.topic, group.subscription
                )));
            }
        }
        for previous in self.current_layout.segments.values() {
            let Some(replacement) = segments.get(&previous.id) else {
                continue;
            };
            if replacement.hash_start != previous.hash_start
                || replacement.hash_end != previous.hash_end
                || replacement.created_at_epoch != previous.created_at_epoch
            {
                return Err(M1FakeError::InvalidLayout(format!(
                    "segment {} changed immutable identity",
                    previous.id
                )));
            }
            let retained_parents: Vec<_> = previous
                .parent_ids
                .iter()
                .filter(|segment_id| !removed.contains(segment_id))
                .copied()
                .collect();
            if replacement.parent_ids != retained_parents {
                return Err(M1FakeError::InvalidLayout(format!(
                    "segment {} rewrote a non-GC parent edge",
                    previous.id
                )));
            }
            let retained_children: Vec<_> = previous
                .child_ids
                .iter()
                .filter(|segment_id| !removed.contains(segment_id))
                .copied()
                .collect();
            if previous.state == pb::SegmentState::Sealed
                && (replacement.state != pb::SegmentState::Sealed
                    || replacement.sealed_at_epoch != previous.sealed_at_epoch
                    || replacement.child_ids != retained_children)
            {
                return Err(M1FakeError::InvalidLayout(format!(
                    "sealed segment {} changed lifecycle identity",
                    previous.id
                )));
            }
            if previous.state == pb::SegmentState::Active
                && replacement.state == pb::SegmentState::Active
                && replacement.child_ids != retained_children
            {
                return Err(M1FakeError::InvalidLayout(format!(
                    "active segment {} changed child identity",
                    previous.id
                )));
            }
        }
        Ok(())
    }

    fn validate_segment_descriptor(
        &self,
        epoch: u64,
        segment: &M1Segment,
    ) -> Result<(), M1FakeError> {
        if segment.hash_start > segment.hash_end || segment.hash_end > HASH_RANGE_END {
            return Err(M1FakeError::InvalidLayout(format!(
                "segment {} has an invalid inclusive range",
                segment.id
            )));
        }
        let is_new = !self.current_layout.segments.contains_key(&segment.id);
        if is_new && self.segment_catalog.contains_key(&segment.id) {
            return Err(M1FakeError::InvalidLayout(format!(
                "garbage-collected segment {} was reintroduced",
                segment.id
            )));
        }
        if segment.created_at_epoch > epoch || (is_new && segment.created_at_epoch == 0) {
            return Err(M1FakeError::InvalidLayout(format!(
                "segment {} has invalid creation epoch {} at layout {epoch}",
                segment.id, segment.created_at_epoch
            )));
        }
        if is_new && segment.created_at_epoch < epoch && segment.parent_ids.is_empty() {
            return Err(M1FakeError::InvalidLayout(format!(
                "segment {} is a backdated disconnected root",
                segment.id
            )));
        }
        match segment.state {
            pb::SegmentState::Active => {
                if segment.sealed_at_epoch.is_some() || !segment.child_ids.is_empty() {
                    return Err(M1FakeError::InvalidLayout(format!(
                        "active segment {} must be an unsealed leaf",
                        segment.id
                    )));
                }
                self.validate_serving_endpoint(segment)
            }
            pb::SegmentState::Sealed => {
                let sealed_at = segment.sealed_at_epoch.ok_or_else(|| {
                    M1FakeError::InvalidLayout(format!(
                        "sealed segment {} has no sealing epoch",
                        segment.id
                    ))
                })?;
                if sealed_at < segment.created_at_epoch || sealed_at > epoch {
                    return Err(M1FakeError::InvalidLayout(format!(
                        "segment {} has invalid sealing epoch {sealed_at}",
                        segment.id
                    )));
                }
                if segment.endpoint.is_some() {
                    self.validate_serving_endpoint(segment)?;
                }
                Ok(())
            }
        }
    }

    fn validate_serving_endpoint(&self, segment: &M1Segment) -> Result<(), M1FakeError> {
        let endpoint = segment.endpoint.ok_or_else(|| {
            M1FakeError::InvalidLayout(format!("served segment {} has no placement", segment.id))
        })?;
        if !matches!(endpoint, Endpoint::Segment(_)) || !self.authorities.contains_key(&endpoint) {
            return Err(M1FakeError::InvalidLayout(format!(
                "segment {} has unknown child placement {endpoint:?}",
                segment.id
            )));
        }
        Ok(())
    }

    fn validate_layout_edges(
        epoch: u64,
        segments: &BTreeMap<u64, M1Segment>,
    ) -> Result<(), M1FakeError> {
        let edge_count: usize = segments
            .values()
            .map(|segment| segment.parent_ids.len())
            .sum();
        if edge_count > MAX_DAG_EDGES {
            return Err(M1FakeError::InvalidLayout(format!(
                "layout exceeds {MAX_DAG_EDGES} ancestry edges"
            )));
        }
        for segment in segments.values() {
            let parents: BTreeSet<_> = segment.parent_ids.iter().copied().collect();
            let children: BTreeSet<_> = segment.child_ids.iter().copied().collect();
            if parents.len() != segment.parent_ids.len()
                || children.len() != segment.child_ids.len()
            {
                return Err(M1FakeError::InvalidLayout(format!(
                    "segment {} repeats an ancestry edge",
                    segment.id
                )));
            }
            for parent_id in &segment.parent_ids {
                let parent = segments.get(parent_id).ok_or_else(|| {
                    M1FakeError::InvalidLayout(format!(
                        "segment {} references missing parent {parent_id}",
                        segment.id
                    ))
                })?;
                if *parent_id == segment.id || !parent.child_ids.contains(&segment.id) {
                    return Err(M1FakeError::InvalidLayout(format!(
                        "edge {parent_id}->{} is not reciprocal",
                        segment.id
                    )));
                }
                if parent.state != pb::SegmentState::Sealed
                    || parent.sealed_at_epoch != Some(segment.created_at_epoch)
                    || segment.created_at_epoch > epoch
                {
                    return Err(M1FakeError::InvalidLayout(format!(
                        "edge {parent_id}->{} has inconsistent lifecycle epochs",
                        segment.id
                    )));
                }
            }
            for child_id in &segment.child_ids {
                let child = segments.get(child_id).ok_or_else(|| {
                    M1FakeError::InvalidLayout(format!(
                        "segment {} references missing child {child_id}",
                        segment.id
                    ))
                })?;
                if !child.parent_ids.contains(&segment.id) {
                    return Err(M1FakeError::InvalidLayout(format!(
                        "edge {}->{child_id} is not reciprocal",
                        segment.id
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_layout_wire_size(
        &self,
        epoch: u64,
        segments: &BTreeMap<u64, M1Segment>,
    ) -> Result<(), M1FakeError> {
        let snapshot = LayoutSnapshot {
            epoch,
            segments: segments.clone(),
        };
        let command = self.layout_command(0, &snapshot);
        let mut encoded = BytesMut::new();
        encode_command(&mut encoded, &command)?;
        if encoded.len() > MAX_FRAME_SIZE {
            return Err(M1FakeError::InvalidLayout(format!(
                "serialized layout exceeds {MAX_FRAME_SIZE} bytes"
            )));
        }
        Ok(())
    }

    fn current_assignment_segment_ids(&self, group: &GroupKey) -> BTreeSet<u64> {
        self.legal_assignment_segment_ids(&self.current_layout, group)
    }

    fn record_current_assignment_context(&mut self, group: &GroupKey) {
        let legal_segments = self.current_assignment_segment_ids(group);
        let consumer_names: BTreeSet<String> = self
            .memberships
            .values()
            .filter(|membership| membership.registered && &membership.group == group)
            .map(|membership| membership.consumer_name.clone())
            .collect();
        self.assignment_contexts
            .entry((group.clone(), self.current_layout.epoch))
            .and_modify(|context| {
                context
                    .legal_segments
                    .extend(legal_segments.iter().copied());
                context
                    .consumer_names
                    .extend(consumer_names.iter().cloned());
            })
            .or_insert(AssignmentContext {
                legal_segments,
                consumer_names,
            });
    }

    fn legal_assignment_segment_ids(
        &self,
        snapshot: &LayoutSnapshot,
        group: &GroupKey,
    ) -> BTreeSet<u64> {
        snapshot
            .segments
            .values()
            .filter(|segment| {
                segment.state == pb::SegmentState::Sealed
                    || segment.parent_ids.iter().all(|parent_id| {
                        self.completed_segments
                            .contains(&(group.clone(), *parent_id))
                    })
            })
            .map(|segment| segment.id)
            .collect()
    }

    fn assignment_segment_ids(&self, layout_epoch: u64, group: &GroupKey) -> BTreeSet<u64> {
        if layout_epoch < self.current_layout.epoch {
            return self
                .assignment_contexts
                .get(&(group.clone(), layout_epoch))
                .map(|context| context.legal_segments.clone())
                .unwrap_or_default();
        }
        self.layout_history
            .get(&layout_epoch)
            .map(|snapshot| self.legal_assignment_segment_ids(snapshot, group))
            .unwrap_or_default()
    }

    fn connected_state(&self, connection: ConnectionId) -> Result<&ConnectionState, M1FakeError> {
        let state = self
            .connections
            .get(&connection)
            .ok_or(M1FakeError::UnknownConnection(connection))?;
        if state.connected {
            Ok(state)
        } else {
            Err(M1FakeError::Disconnected(connection))
        }
    }

    fn connected_state_mut(
        &mut self,
        connection: ConnectionId,
    ) -> Result<&mut ConnectionState, M1FakeError> {
        let state = self
            .connections
            .get_mut(&connection)
            .ok_or(M1FakeError::UnknownConnection(connection))?;
        if state.connected {
            Ok(state)
        } else {
            Err(M1FakeError::Disconnected(connection))
        }
    }

    fn connection_endpoint(&self, connection: ConnectionId) -> Result<Endpoint, M1FakeError> {
        Ok(self.connected_state(connection)?.endpoint)
    }

    fn require_endpoint(
        &self,
        connection: ConnectionId,
        expected: Endpoint,
        command: pb::base_command::Type,
    ) -> Result<Endpoint, M1FakeError> {
        let actual = self.connection_endpoint(connection)?;
        if actual == expected {
            Ok(actual)
        } else {
            Err(M1FakeError::WrongEndpoint {
                command,
                expected,
                actual,
            })
        }
    }

    fn require_layout_session(
        &self,
        connection: ConnectionId,
        session_id: u64,
    ) -> Result<(), M1FakeError> {
        self.require_endpoint(
            connection,
            Endpoint::Controller,
            pb::base_command::Type::ScalableTopicUpdate,
        )?;
        if !self.layout_sessions.contains(&(connection, session_id)) {
            return Err(invalid(
                pb::base_command::Type::ScalableTopicUpdate,
                format!("unknown layout session {session_id}"),
            ));
        }
        Ok(())
    }

    fn segment_id_for_topic(&self, topic: &str) -> Option<u64> {
        let attachment = parse_canonical_segment_topic(&self.topic, topic).ok()?;
        let segment = self.segment_catalog.get(&attachment.segment_id)?;
        (segment.hash_start == attachment.hash_start
            && segment.hash_end == attachment.hash_end
            && canonical_segment_topic(&self.topic, segment)
                .ok()
                .as_deref()
                == Some(topic))
        .then_some(attachment.segment_id)
    }

    fn take_behavior(
        &mut self,
        endpoint: Endpoint,
        kind: OperationKind,
    ) -> Option<ScriptedBehavior> {
        self.scripts
            .get_mut(&(endpoint, kind))
            .and_then(VecDeque::pop_front)
    }

    fn allocate_pending_id(&mut self) -> PendingOperationId {
        let id = PendingOperationId(self.next_pending_id);
        self.next_pending_id = self.next_pending_id.saturating_add(1);
        id
    }

    fn assignment_pb(
        &self,
        group: &GroupKey,
        layout_epoch: u64,
        segments: &BTreeSet<u64>,
    ) -> Result<pb::ScalableConsumerAssignment, M1FakeError> {
        let snapshot = self.layout_history.get(&layout_epoch).ok_or_else(|| {
            M1FakeError::InvalidAssignment(format!(
                "assignment references unknown layout epoch {layout_epoch}"
            ))
        })?;
        let mut assigned = Vec::with_capacity(segments.len());
        for segment_id in segments {
            let segment = snapshot.segments.get(segment_id).ok_or_else(|| {
                M1FakeError::InvalidAssignment(format!(
                    "segment {segment_id} did not exist at layout epoch {layout_epoch}"
                ))
            })?;
            assigned.push(pb::ScalableAssignedSegment {
                segment_id: *segment_id,
                hash_start: segment.hash_start,
                hash_end: segment.hash_end,
                segment_topic: canonical_segment_topic(&group.topic, segment)?,
            });
        }
        let assignment = pb::ScalableConsumerAssignment {
            layout_epoch,
            segments: assigned,
        };
        Ok(assignment)
    }

    fn queue_scalable_subscribe_response(
        &mut self,
        member: MemberId,
        request_id: u64,
        failure: Option<&BrokerFailure>,
    ) -> Result<(), M1FakeError> {
        let membership = self
            .memberships
            .get(&member)
            .ok_or(M1FakeError::UnknownMember(member))?;
        let assignment = self.assignment_pb(
            &membership.group,
            membership.assignment_epoch,
            &membership.assigned,
        );
        assignment.and_then(|assignment| {
            let command = pb::BaseCommand {
                r#type: pb::base_command::Type::ScalableTopicSubscribeResponse as i32,
                scalable_topic_subscribe_response: Some(
                    pb::CommandScalableTopicSubscribeResponse {
                        request_id,
                        error: failure.map(|failure| failure.error as i32),
                        message: failure.map(|failure| failure.message.clone()),
                        assignment: failure.is_none().then_some(assignment),
                    },
                ),
                ..Default::default()
            };
            self.queue_command(member.connection, &command)
        })
    }

    fn queue_scalable_subscribe_failure(
        &mut self,
        connection: ConnectionId,
        request_id: u64,
        failure: &BrokerFailure,
    ) -> Result<(), M1FakeError> {
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::ScalableTopicSubscribeResponse as i32,
            scalable_topic_subscribe_response: Some(pb::CommandScalableTopicSubscribeResponse {
                request_id,
                error: Some(failure.error as i32),
                message: Some(failure.message.clone()),
                assignment: None,
            }),
            ..Default::default()
        };
        self.queue_command(connection, &command)
    }

    fn queue_assignment_update(&mut self, member: MemberId) -> Result<(), M1FakeError> {
        let membership = self
            .memberships
            .get(&member)
            .ok_or(M1FakeError::UnknownMember(member))?;
        let assignment = self.assignment_pb(
            &membership.group,
            membership.assignment_epoch,
            &membership.assigned,
        );
        assignment.and_then(|assignment| self.queue_assignment_frame(member, assignment))
    }

    fn queue_assignment_frame(
        &mut self,
        member: MemberId,
        assignment: pb::ScalableConsumerAssignment,
    ) -> Result<(), M1FakeError> {
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::ScalableTopicAssignmentUpdate as i32,
            scalable_topic_assignment_update: Some(pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: member.consumer_id,
                assignment,
            }),
            ..Default::default()
        };
        self.queue_command(member.connection, &command)
    }

    fn queue_layout(
        &mut self,
        connection: ConnectionId,
        session_id: u64,
        snapshot: &LayoutSnapshot,
    ) -> Result<(), M1FakeError> {
        let command = self.layout_command(session_id, snapshot);
        self.queue_command(connection, &command)
    }

    fn layout_command(&self, session_id: u64, snapshot: &LayoutSnapshot) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
            scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
                session_id,
                dag: Some(self.layout_pb(snapshot)),
                error: None,
                message: None,
                resolved_topic_name: Some(self.topic.clone()),
            }),
            ..Default::default()
        }
    }

    fn layout_pb(&self, snapshot: &LayoutSnapshot) -> pb::ScalableTopicDag {
        let segments: Vec<_> = snapshot.segments.values().map(M1Segment::to_pb).collect();
        let segment_brokers = snapshot
            .segments
            .values()
            .filter(|segment| segment.state == pb::SegmentState::Active)
            .filter_map(|segment| {
                let authorities = self.authorities.get(&segment.endpoint?)?;
                Some(pb::SegmentBrokerAddress {
                    segment_id: segment.id,
                    broker_url: authorities.plaintext.clone(),
                    broker_url_tls: Some(authorities.tls.clone()),
                })
            })
            .collect();
        let controller = self.authorities.get(&Endpoint::Controller);
        pb::ScalableTopicDag {
            epoch: snapshot.epoch,
            segments,
            segment_brokers,
            controller_broker_url: controller.map(|authority| authority.plaintext.clone()),
            controller_broker_url_tls: controller.map(|authority| authority.tls.clone()),
        }
    }

    fn queue_success(
        &mut self,
        connection: ConnectionId,
        request_id: u64,
    ) -> Result<(), M1FakeError> {
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id,
                schema: None,
            }),
            ..Default::default()
        };
        self.queue_command(connection, &command)
    }

    fn queue_error(
        &mut self,
        connection: ConnectionId,
        request_id: u64,
        failure: &BrokerFailure,
    ) -> Result<(), M1FakeError> {
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id,
                error: failure.error as i32,
                message: failure.message.clone(),
            }),
            ..Default::default()
        };
        self.queue_command(connection, &command)
    }

    fn queue_ack_response(
        &mut self,
        connection: ConnectionId,
        consumer_id: u64,
        request_id: Option<u64>,
        txn_id: Option<magnetar_proto::TxnId>,
        failure: Option<&BrokerFailure>,
    ) -> Result<(), M1FakeError> {
        if request_id.is_none() && failure.is_none() {
            return Ok(());
        }
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::AckResponse as i32,
            ack_response: Some(pb::CommandAckResponse {
                consumer_id,
                txnid_least_bits: txn_id.map(|txn_id| txn_id.least_sig_bits),
                txnid_most_bits: txn_id.map(|txn_id| txn_id.most_sig_bits),
                error: failure.map(|failure| failure.error as i32),
                message: failure.map(|failure| failure.message.clone()),
                request_id,
            }),
            ..Default::default()
        };
        self.queue_command(connection, &command)
    }

    fn queue_get_schema_response(
        &mut self,
        connection: ConnectionId,
        request_id: u64,
        topic: String,
        schema_version: Option<Bytes>,
        failure: Option<&BrokerFailure>,
    ) -> Result<(), M1FakeError> {
        self.queue_command(
            connection,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::GetSchemaResponse as i32,
                get_schema_response: Some(pb::CommandGetSchemaResponse {
                    request_id,
                    error_code: failure.map(|failure| failure.error as i32),
                    error_message: failure.map(|failure| failure.message.clone()),
                    schema: failure.is_none().then_some(pb::Schema {
                        name: topic,
                        schema_data: Bytes::new(),
                        r#type: pb::schema::Type::None as i32,
                        properties: Vec::new(),
                    }),
                    schema_version,
                }),
                ..Default::default()
            },
        )
    }

    fn queue_add_subscription_response(
        &mut self,
        connection: ConnectionId,
        request_id: u64,
        txn_id: magnetar_proto::TxnId,
        failure: Option<&BrokerFailure>,
    ) -> Result<(), M1FakeError> {
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::AddSubscriptionToTxnResponse as i32,
            add_subscription_to_txn_response: Some(pb::CommandAddSubscriptionToTxnResponse {
                request_id,
                txnid_least_bits: Some(txn_id.least_sig_bits),
                txnid_most_bits: Some(txn_id.most_sig_bits),
                error: failure.map(|failure| failure.error as i32),
                message: failure.map(|failure| failure.message.clone()),
            }),
            ..Default::default()
        };
        self.queue_command(connection, &command)
    }

    fn queue_end_transaction_response(
        &mut self,
        connection: ConnectionId,
        request_id: u64,
        txn_id: magnetar_proto::TxnId,
        failure: Option<&BrokerFailure>,
    ) -> Result<(), M1FakeError> {
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::EndTxnResponse as i32,
            end_txn_response: Some(pb::CommandEndTxnResponse {
                request_id,
                txnid_least_bits: Some(txn_id.least_sig_bits),
                txnid_most_bits: Some(txn_id.most_sig_bits),
                error: failure.map(|failure| failure.error as i32),
                message: failure.map(|failure| failure.message.clone()),
            }),
            ..Default::default()
        };
        self.queue_command(connection, &command)
    }

    fn queue_command(
        &mut self,
        connection: ConnectionId,
        command: &pb::BaseCommand,
    ) -> Result<(), M1FakeError> {
        let mut bytes = BytesMut::new();
        encode_command(&mut bytes, command)?;
        self.queue_bytes(connection, bytes.freeze())?;
        if let Ok(kind) = pb::base_command::Type::try_from(command.r#type) {
            self.broker_frames.push(BrokerFrameObservation {
                connection,
                command: kind,
            });
        }
        Ok(())
    }

    fn queue_bytes(&mut self, connection: ConnectionId, bytes: Bytes) -> Result<(), M1FakeError> {
        self.connected_state_mut(connection)?
            .output
            .push_back(bytes);
        Ok(())
    }

    fn dispatch_consumer(&mut self, member: MemberId) -> Result<(), M1FakeError> {
        let mut deliveries = Vec::new();
        let sparse_acks = self
            .child_consumers
            .get(&member)
            .and_then(|consumer| self.child_subscriptions.get(&consumer.key))
            .map(|subscription| subscription.individually_acked.clone())
            .unwrap_or_default();
        while let Some(consumer) = self.child_consumers.get_mut(&member) {
            if consumer.permits == 0 || consumer.closing {
                break;
            }
            let ledger_len = self.ledgers.get(&consumer.segment_id).map_or(0, Vec::len);
            while consumer.delivery_cursor < ledger_len
                && sparse_acks.contains(&consumer.delivery_cursor)
            {
                consumer.delivery_cursor = consumer.delivery_cursor.saturating_add(1);
            }
            let (index, redelivery_count) = if let Some(index) = consumer.redeliver.pop_front() {
                let count = consumer.redelivery_counts.entry(index).or_insert(0);
                *count = count.saturating_add(1);
                (index, *count)
            } else if consumer.delivery_cursor < ledger_len {
                let index = consumer.delivery_cursor;
                consumer.delivery_cursor = consumer.delivery_cursor.saturating_add(1);
                (index, 0)
            } else {
                break;
            };
            consumer.permits -= 1;
            consumer.unacked.insert(index);
            if let Some(stored) = self
                .ledgers
                .get(&consumer.segment_id)
                .and_then(|ledger| ledger.get(index))
            {
                deliveries.push((consumer.segment_id, index, redelivery_count, stored.clone()));
            }
        }
        for (segment_id, index, redelivery_count, stored) in deliveries {
            let command = pb::BaseCommand {
                r#type: pb::base_command::Type::Message as i32,
                message: Some(pb::CommandMessage {
                    consumer_id: member.consumer_id,
                    message_id: message_id(segment_id, index as u64),
                    redelivery_count: Some(redelivery_count),
                    ack_set: stored.ack_set,
                    consumer_epoch: None,
                }),
                ..Default::default()
            };
            let mut bytes = BytesMut::new();
            encode_payload(&mut bytes, &command, &stored.metadata, &stored.payload)?;
            self.queue_bytes(member.connection, bytes.freeze())?;
        }
        self.maybe_emit_terminal(member)
    }

    fn validate_delivered_ids(
        consumer: &ChildConsumer,
        message_ids: &[pb::MessageIdData],
        command: pb::base_command::Type,
    ) -> Result<Vec<usize>, M1FakeError> {
        let mut indices = Vec::with_capacity(message_ids.len());
        let mut components = BTreeSet::new();
        let mut physical_entries = BTreeSet::new();
        for delivered_id in message_ids {
            if delivered_id.ledger_id != consumer.segment_id {
                return Err(invalid(command, "message id belongs to another segment"));
            }
            let index = usize::try_from(delivered_id.entry_id)
                .map_err(|_| invalid(command, "message entry does not fit usize"))?;
            if delivered_id.partition != Some(-1) {
                return Err(invalid(
                    command,
                    "message id does not match the delivered fake partition",
                ));
            }
            if !consumer.unacked.contains(&index) {
                return Err(invalid(
                    command,
                    format!(
                        "message {}/{index} is not delivered and unacked",
                        consumer.segment_id
                    ),
                ));
            }
            if let Some(first_chunk) = delivered_id.first_chunk_message_id.as_deref() {
                if first_chunk.ledger_id != consumer.segment_id || first_chunk.partition != Some(-1)
                {
                    return Err(invalid(
                        command,
                        "first chunk id does not match the delivered fake segment",
                    ));
                }
                let first_index = usize::try_from(first_chunk.entry_id)
                    .map_err(|_| invalid(command, "first chunk entry does not fit usize"))?;
                if first_index > index
                    || (first_index..=index).any(|chunk| !consumer.unacked.contains(&chunk))
                {
                    return Err(invalid(
                        command,
                        "chunk range is not entirely delivered and unacked",
                    ));
                }
                for chunk in first_index..index {
                    if physical_entries.insert(chunk) {
                        indices.push(chunk);
                    }
                }
            }
            if !components.insert((index, delivered_id.batch_index.unwrap_or(-1))) {
                return Err(invalid(command, "message-id vector contains a duplicate"));
            }
            if physical_entries.insert(index) {
                indices.push(index);
            }
        }
        Ok(indices)
    }

    fn require_open_transaction(
        &self,
        txn_id: magnetar_proto::TxnId,
        command: pb::base_command::Type,
    ) -> Result<(), M1FakeError> {
        match self.transactions.get(&txn_id) {
            Some(transaction) if transaction.state == FakeTransactionState::Open => Ok(()),
            Some(transaction) => Err(invalid(
                command,
                format!("transaction {txn_id} is {:?}", transaction.state),
            )),
            None => Err(invalid(command, format!("unknown transaction {txn_id}"))),
        }
    }

    fn register_transaction_subscription(
        &mut self,
        txn_id: magnetar_proto::TxnId,
        key: &ChildSubscriptionKey,
    ) -> Result<(), M1FakeError> {
        self.require_open_transaction(txn_id, pb::base_command::Type::AddSubscriptionToTxn)?;
        self.transactions
            .get_mut(&txn_id)
            .into_iter()
            .for_each(|transaction| {
                transaction.registered_subscriptions.insert(key.clone());
            });
        Ok(())
    }

    fn apply_or_stage_ack(
        &mut self,
        member: MemberId,
        ack_type: pb::command_ack::AckType,
        indices: &[usize],
        fence: &ChildFence,
        txn_id: Option<magnetar_proto::TxnId>,
    ) -> Result<(), M1FakeError> {
        let Some(txn_id) = txn_id else {
            self.apply_ack(member, ack_type, indices);
            return Ok(());
        };
        self.require_open_transaction(txn_id, pb::base_command::Type::Ack)?;
        let transaction = self
            .transactions
            .get_mut(&txn_id)
            .ok_or_else(|| invalid(pb::base_command::Type::Ack, "unknown transaction"))?;
        if transaction.staged_acknowledgements.iter().any(|staged| {
            staged.fence == *fence && staged.indices.iter().any(|index| indices.contains(index))
        }) {
            return Err(invalid(
                pb::base_command::Type::Ack,
                "transaction repeats a staged acknowledgement",
            ));
        }
        transaction
            .staged_acknowledgements
            .push(StagedTransactionalAck {
                member,
                ack_type,
                indices: indices.to_vec(),
                fence: fence.clone(),
            });
        Ok(())
    }

    fn apply_seek(&mut self, member: MemberId, target: usize) -> Result<(), M1FakeError> {
        let (segment_id, key, group) = self
            .child_consumers
            .get(&member)
            .map(|consumer| {
                (
                    consumer.segment_id,
                    consumer.key.clone(),
                    consumer.group.clone(),
                )
            })
            .ok_or_else(|| invalid(pb::base_command::Type::Seek, "unknown child consumer"))?;
        let subscription = self.child_subscriptions.entry(key).or_default();
        subscription.durable_cursor = Some(target);
        subscription.individually_acked.clear();
        self.completed_segments.remove(&(group, segment_id));
        // Pulsar tears down the broker-side consumer while applying seek. The
        // production client re-subscribes this same id before restoring FLOW.
        self.remove_child_consumer(member);
        Ok(())
    }

    fn apply_ack(
        &mut self,
        member: MemberId,
        ack_type: pb::command_ack::AckType,
        indices: &[usize],
    ) {
        // Activation installs the consumer, subscription, and durable cursor as
        // one state transition, so public commands cannot observe a partial tuple.
        let consumers = &mut self.child_consumers;
        let subscriptions = &mut self.child_subscriptions;
        consumers.get_mut(&member).into_iter().for_each(|consumer| {
            subscriptions
                .get_mut(&consumer.key)
                .into_iter()
                .for_each(|subscription| {
                    subscription.durable_cursor.iter_mut().for_each(
                        |durable_cursor| match ack_type {
                            pb::command_ack::AckType::Individual => {
                                for index in indices {
                                    consumer.unacked.remove(index);
                                    consumer.redeliver.retain(|candidate| candidate != index);
                                    subscription.individually_acked.insert(*index);
                                }
                                while subscription.individually_acked.remove(durable_cursor) {
                                    *durable_cursor = (*durable_cursor).saturating_add(1);
                                }
                            }
                            pb::command_ack::AckType::Cumulative => {
                                let through = indices[0];
                                consumer.unacked.retain(|index| *index > through);
                                consumer.redeliver.retain(|index| *index > through);
                                *durable_cursor = (*durable_cursor).max(through.saturating_add(1));
                                subscription
                                    .individually_acked
                                    .retain(|index| *index >= *durable_cursor);
                            }
                        },
                    );
                });
        });
    }

    fn maybe_emit_terminal(&mut self, member: MemberId) -> Result<(), M1FakeError> {
        let should_emit = self.child_consumers.get(&member).is_some_and(|consumer| {
            self.terminal_segments.contains(&consumer.segment_id)
                && !consumer.terminal_sent
                && consumer.redeliver.is_empty()
                && consumer.delivery_cursor
                    >= self.ledgers.get(&consumer.segment_id).map_or(0, Vec::len)
        });
        if !should_emit {
            return Ok(());
        }
        let consumer = self
            .child_consumers
            .get_mut(&member)
            .ok_or_else(|| invalid(pb::base_command::Type::Message, "unknown child consumer"))?;
        consumer.terminal_sent = true;
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::ReachedEndOfTopic as i32,
            reached_end_of_topic: Some(pb::CommandReachedEndOfTopic {
                consumer_id: member.consumer_id,
            }),
            ..Default::default()
        };
        self.queue_command(member.connection, &command)?;
        self.refresh_segment_completion(member);
        Ok(())
    }

    fn refresh_segment_completion(&mut self, member: MemberId) {
        let completed = self.child_consumers.get(&member).and_then(|consumer| {
            let fence = child_fence(member, consumer);
            let ack_pending = self.pending.values().any(|operation| {
                matches!(operation, PendingOperation::Ack { fence: pending, .. } if pending == &fence)
            });
            (consumer.terminal_sent && consumer.unacked.is_empty() && !ack_pending)
                .then(|| (consumer.group.clone(), consumer.segment_id))
        });
        self.completed_segments.extend(completed);
    }

    fn child_fence(&self, member: MemberId) -> Result<ChildFence, M1FakeError> {
        self.child_consumers
            .get(&member)
            .map(|consumer| child_fence(member, consumer))
            .ok_or_else(|| invalid(pb::base_command::Type::Ack, "unknown child consumer"))
    }

    fn child_fence_matches(&self, member: MemberId, expected: &ChildFence) -> bool {
        self.child_consumers
            .get(&member)
            .is_some_and(|consumer| child_fence(member, consumer) == *expected)
            && self.memberships.contains_key(&expected.controller_member)
    }

    fn require_child_fence(
        &self,
        id: PendingOperationId,
        member: MemberId,
        expected: &ChildFence,
    ) -> Result<(), M1FakeError> {
        if self.child_fence_matches(member, expected) {
            Ok(())
        } else {
            Err(M1FakeError::StalePending(id))
        }
    }

    fn require_close_fence(
        &mut self,
        id: PendingOperationId,
        member: MemberId,
        expected: &ChildFence,
    ) -> Result<(), M1FakeError> {
        if self.child_fence_matches(member, expected) {
            return Ok(());
        }
        let same_child = self
            .child_consumers
            .get(&member)
            .is_some_and(|consumer| child_fence(member, consumer) == *expected);
        if same_child && let Some(consumer) = self.child_consumers.get_mut(&member) {
            consumer.closing = false;
        }
        Err(M1FakeError::StalePending(id))
    }

    fn pending_open_is_current(
        &self,
        pending_id: PendingOperationId,
        activation: &ChildActivation,
    ) -> bool {
        self.child_subscriptions
            .get(&activation.key)
            .is_some_and(|subscription| {
                matches!(subscription.owner, Some(ChildOwner::Pending(id)) if id == pending_id)
            })
            && self
                .segment_owners
                .get(&(activation.group.clone(), activation.segment_id))
                == Some(&activation.controller_member)
            && self
                .memberships
                .get(&activation.controller_member)
                .is_some_and(|membership| {
                    membership.registered
                        && membership.group == activation.group
                        && membership.assigned.contains(&activation.segment_id)
                })
            && self.segment_endpoint(activation.segment_id) == Some(activation.serving_endpoint)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CanonicalAttachment {
    hash_start: u32,
    hash_end: u32,
    segment_id: u64,
}

fn default_authorities() -> BTreeMap<Endpoint, EndpointAuthorities> {
    BTreeMap::from([
        (
            Endpoint::Controller,
            EndpointAuthorities::new(
                "pulsar://controller.m1.test:6650",
                "pulsar+ssl://controller.m1.test:6651",
            ),
        ),
        (
            Endpoint::Segment(1),
            EndpointAuthorities::new(
                "pulsar://segment-1.m1.test:6650",
                "pulsar+ssl://segment-1.m1.test:6651",
            ),
        ),
        (
            Endpoint::Segment(2),
            EndpointAuthorities::new(
                "pulsar://segment-2.m1.test:6650",
                "pulsar+ssl://segment-2.m1.test:6651",
            ),
        ),
    ])
}

fn validate_scalable_topic(topic: &str) -> Result<(), M1FakeError> {
    let Some(path) = topic.strip_prefix("topic://") else {
        return Err(M1FakeError::InvalidLayout(
            "the fake topic must use `topic://`".to_owned(),
        ));
    };
    let parts: Vec<_> = path.split('/').collect();
    if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
        return Err(M1FakeError::InvalidLayout(
            "the fake topic must be `topic://tenant/namespace/topic`".to_owned(),
        ));
    }
    Ok(())
}

fn validate_authorities(
    endpoint: Endpoint,
    authorities: &EndpointAuthorities,
) -> Result<(), M1FakeError> {
    validate_binary_url(endpoint, &authorities.plaintext, "pulsar://")?;
    validate_binary_url(endpoint, &authorities.tls, "pulsar+ssl://")?;
    Ok(())
}

fn validate_binary_url(endpoint: Endpoint, url: &str, scheme: &str) -> Result<(), M1FakeError> {
    let Some(authority) = url.strip_prefix(scheme) else {
        return Err(M1FakeError::InvalidLayout(format!(
            "endpoint {endpoint:?} authority must use `{scheme}`"
        )));
    };
    if authority.is_empty()
        || authority.contains('@')
        || authority.contains('/')
        || authority.contains('?')
        || authority.contains('#')
    {
        return Err(M1FakeError::InvalidLayout(format!(
            "endpoint {endpoint:?} carries a malformed authority"
        )));
    }
    Ok(())
}

fn canonical_segment_topic(
    scalable_topic: &str,
    segment: &M1Segment,
) -> Result<String, M1FakeError> {
    validate_scalable_topic(scalable_topic)?;
    let path = scalable_topic
        .strip_prefix("topic://")
        .ok_or_else(|| M1FakeError::InvalidAssignment("missing topic scheme".to_owned()))?;
    Ok(format!(
        "segment://{path}/{:04x}-{:04x}-{}",
        segment.hash_start, segment.hash_end, segment.id
    ))
}

fn parse_canonical_segment_topic(
    scalable_topic: &str,
    attachment: &str,
) -> Result<CanonicalAttachment, M1FakeError> {
    validate_scalable_topic(scalable_topic)?;
    let path = scalable_topic
        .strip_prefix("topic://")
        .ok_or_else(|| M1FakeError::InvalidAssignment("missing topic scheme".to_owned()))?;
    let prefix = format!("segment://{path}/");
    let suffix = attachment.strip_prefix(&prefix).ok_or_else(|| {
        M1FakeError::InvalidAssignment("attachment names another scalable topic".to_owned())
    })?;
    let mut parts = suffix.split('-');
    let start = parts
        .next()
        .ok_or_else(|| M1FakeError::InvalidAssignment("attachment has no hash start".to_owned()))?;
    let end = parts
        .next()
        .ok_or_else(|| M1FakeError::InvalidAssignment("attachment has no hash end".to_owned()))?;
    let id = parts
        .next()
        .ok_or_else(|| M1FakeError::InvalidAssignment("attachment has no segment id".to_owned()))?;
    if parts.next().is_some() {
        return Err(M1FakeError::InvalidAssignment(
            "attachment has trailing components".to_owned(),
        ));
    }
    let parsed = CanonicalAttachment {
        hash_start: u32::from_str_radix(start, 16).map_err(|_| {
            M1FakeError::InvalidAssignment("attachment hash start is not hexadecimal".to_owned())
        })?,
        hash_end: u32::from_str_radix(end, 16).map_err(|_| {
            M1FakeError::InvalidAssignment("attachment hash end is not hexadecimal".to_owned())
        })?,
        segment_id: id.parse().map_err(|_| {
            M1FakeError::InvalidAssignment("attachment segment id is not decimal".to_owned())
        })?,
    };
    let canonical_suffix = format!(
        "{:04x}-{:04x}-{}",
        parsed.hash_start, parsed.hash_end, parsed.segment_id
    );
    if suffix != canonical_suffix {
        return Err(M1FakeError::InvalidAssignment(
            "attachment is not in canonical lowercase form".to_owned(),
        ));
    }
    Ok(parsed)
}

fn validate_acyclic(segments: &BTreeMap<u64, M1Segment>) -> Result<(), M1FakeError> {
    let mut indegree: BTreeMap<_, _> = segments
        .values()
        .map(|segment| (segment.id, segment.parent_ids.len()))
        .collect();
    let mut ready: VecDeque<_> = indegree
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(*id))
        .collect();
    let mut depths: BTreeMap<_, usize> = ready.iter().map(|id| (*id, 0)).collect();
    let mut visited = 0usize;
    while let Some(segment_id) = ready.pop_front() {
        visited = visited.saturating_add(1);
        let depth = depths.get(&segment_id).copied().unwrap_or(0);
        let child_ids = segments
            .get(&segment_id)
            .map(|segment| segment.child_ids.clone())
            .unwrap_or_default();
        for child_id in child_ids {
            let child_depth = depth.saturating_add(1);
            if child_depth > MAX_DAG_ANCESTRY_DEPTH {
                return Err(M1FakeError::InvalidLayout(format!(
                    "layout exceeds ancestry depth {MAX_DAG_ANCESTRY_DEPTH}"
                )));
            }
            depths
                .entry(child_id)
                .and_modify(|current| *current = (*current).max(child_depth))
                .or_insert(child_depth);
            let became_ready = indegree.get_mut(&child_id).is_some_and(|degree| {
                *degree = degree.saturating_sub(1);
                *degree == 0
            });
            if became_ready {
                ready.push_back(child_id);
            }
        }
    }
    if visited != segments.len() {
        return Err(M1FakeError::InvalidLayout(
            "segment graph contains a cycle".to_owned(),
        ));
    }
    Ok(())
}

fn validate_active_leaf_coverage(segments: &BTreeMap<u64, M1Segment>) -> Result<(), M1FakeError> {
    let active: Vec<_> = segments
        .values()
        .filter(|segment| segment.state == pb::SegmentState::Active)
        .collect();
    ranges_cover(0, HASH_RANGE_END, &active, "active leaves")
}

fn validate_transition_ranges(segments: &BTreeMap<u64, M1Segment>) -> Result<(), M1FakeError> {
    for parent in segments.values() {
        if parent.child_ids.len() > 1 {
            let children: Vec<_> = parent
                .child_ids
                .iter()
                .filter_map(|id| segments.get(id))
                .collect();
            if children
                .iter()
                .any(|child| child.parent_ids.as_slice() != [parent.id])
            {
                return Err(M1FakeError::InvalidLayout(format!(
                    "split parent {} has a child with conflicting parents",
                    parent.id
                )));
            }
            ranges_cover(
                parent.hash_start,
                parent.hash_end,
                &children,
                &format!("split parent {}", parent.id),
            )?;
        } else if let Some(child_id) = parent.child_ids.first() {
            let child = segments
                .get(child_id)
                .ok_or_else(|| M1FakeError::InvalidLayout(format!("missing child {child_id}")))?;
            if child.parent_ids.len() == 1
                && (child.hash_start != parent.hash_start || child.hash_end != parent.hash_end)
            {
                return Err(M1FakeError::InvalidLayout(format!(
                    "one-to-one transition {}->{child_id} changes hash coverage",
                    parent.id
                )));
            }
        }
    }
    for child in segments
        .values()
        .filter(|segment| segment.parent_ids.len() > 1)
    {
        let parents: Vec<_> = child
            .parent_ids
            .iter()
            .filter_map(|id| segments.get(id))
            .collect();
        ranges_cover(
            child.hash_start,
            child.hash_end,
            &parents,
            &format!("merge child {}", child.id),
        )?;
    }
    Ok(())
}

fn ranges_cover(
    start: u32,
    end: u32,
    segments: &[&M1Segment],
    context: &str,
) -> Result<(), M1FakeError> {
    let mut ranges = segments.to_vec();
    ranges.sort_by_key(|segment| segment.hash_start);
    if ranges.first().map(|segment| segment.hash_start) != Some(start)
        || ranges.last().map(|segment| segment.hash_end) != Some(end)
    {
        return Err(M1FakeError::InvalidLayout(format!(
            "{context} do not cover {start:04x}-{end:04x}"
        )));
    }
    for adjacent in ranges.windows(2) {
        if adjacent[0].hash_end.checked_add(1) != Some(adjacent[1].hash_start) {
            return Err(M1FakeError::InvalidLayout(format!(
                "{context} contain a gap or overlap"
            )));
        }
    }
    Ok(())
}

fn transitive_ancestors(
    segments: &BTreeMap<u64, M1Segment>,
    segment_id: u64,
) -> Result<BTreeSet<u64>, M1FakeError> {
    let segment = segments
        .get(&segment_id)
        .ok_or(M1FakeError::UnknownSegment(segment_id))?;
    let mut ancestors = BTreeSet::new();
    let mut pending: Vec<_> = segment.parent_ids.clone();
    while let Some(parent_id) = pending.pop() {
        let newly_seen = ancestors.insert(parent_id);
        pending.extend(
            newly_seen
                .then(|| segments.get(&parent_id))
                .flatten()
                .into_iter()
                .flat_map(|parent| parent.parent_ids.iter().copied()),
        );
    }
    Ok(ancestors)
}

fn child_fence(member: MemberId, consumer: &ChildConsumer) -> ChildFence {
    ChildFence {
        segment_id: consumer.segment_id,
        key: consumer.key.clone(),
        controller_member: consumer.controller_member,
        serving_endpoint: consumer.serving_endpoint,
        generation: consumer.generation,
        child_incarnation: member.connection,
    }
}

fn invalid(command: pb::base_command::Type, reason: impl Into<String>) -> M1FakeError {
    M1FakeError::InvalidCommand {
        command,
        reason: reason.into(),
    }
}

fn message_id(segment_id: u64, entry_id: u64) -> pb::MessageIdData {
    pb::MessageIdData {
        ledger_id: segment_id,
        entry_id,
        partition: Some(-1),
        batch_index: Some(-1),
        ack_set: Vec::new(),
        batch_size: Some(0),
        first_chunk_message_id: None,
    }
}

impl M1FakeCluster {
    fn command_resource(
        &self,
        connection: ConnectionId,
        frame: &Frame,
        kind: pb::base_command::Type,
    ) -> Option<String> {
        let child_topic = |consumer_id| {
            self.child_consumers
                .get(&MemberId::new(connection, consumer_id))
                .map_or_else(
                    || consumer_id.to_string(),
                    |consumer| consumer.key.topic.clone(),
                )
        };
        match kind {
            pb::base_command::Type::ScalableTopicLookup => frame
                .command
                .scalable_topic_lookup
                .as_ref()
                .map(|command| command.topic.clone()),
            pb::base_command::Type::ScalableTopicSubscribe => frame
                .command
                .scalable_topic_subscribe
                .as_ref()
                .map(|command| format!("{}:{}", command.topic, command.consumer_id)),
            pb::base_command::Type::Lookup => frame
                .command
                .lookup_topic
                .as_ref()
                .map(|command| command.topic.clone()),
            pb::base_command::Type::Subscribe => frame
                .command
                .subscribe
                .as_ref()
                .map(|command| format!("{}:{}", command.topic, command.consumer_id)),
            pb::base_command::Type::Flow => frame
                .command
                .flow
                .as_ref()
                .map(|command| child_topic(command.consumer_id)),
            pb::base_command::Type::Ack => frame
                .command
                .ack
                .as_ref()
                .map(|command| child_topic(command.consumer_id)),
            pb::base_command::Type::Seek => frame
                .command
                .seek
                .as_ref()
                .map(|command| child_topic(command.consumer_id)),
            pb::base_command::Type::CloseConsumer => frame
                .command
                .close_consumer
                .as_ref()
                .map(|command| child_topic(command.consumer_id)),
            pb::base_command::Type::AddSubscriptionToTxn => frame
                .command
                .add_subscription_to_txn
                .as_ref()
                .and_then(|command| command.subscription.first())
                .map(|subscription| {
                    format!("{}:{}", subscription.topic, subscription.subscription)
                }),
            pb::base_command::Type::EndTxn => frame
                .command
                .end_txn
                .as_ref()
                .and_then(|command| command.txn_action)
                .and_then(|action| pb::TxnAction::try_from(action).ok())
                .map(|action| action.as_str_name().to_ascii_lowercase()),
            _ => None,
        }
    }
}

fn transaction_id(
    command: pb::base_command::Type,
    most_sig_bits: Option<u64>,
    least_sig_bits: Option<u64>,
) -> Result<magnetar_proto::TxnId, M1FakeError> {
    optional_transaction_id(command, most_sig_bits, least_sig_bits)?
        .ok_or_else(|| invalid(command, "missing transaction id"))
}

fn optional_transaction_id(
    command: pb::base_command::Type,
    most_sig_bits: Option<u64>,
    least_sig_bits: Option<u64>,
) -> Result<Option<magnetar_proto::TxnId>, M1FakeError> {
    match (most_sig_bits, least_sig_bits) {
        (Some(most), Some(least)) => Ok(Some(magnetar_proto::TxnId::new(most, least))),
        (None, None) => Ok(None),
        _ => Err(invalid(
            command,
            "transaction id must carry both wire halves",
        )),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    #![allow(clippy::panic)]
    #![allow(clippy::too_many_lines)]

    use super::*;

    fn send(
        cluster: &mut M1FakeCluster,
        connection: ConnectionId,
        command: &pb::BaseCommand,
    ) -> Result<(), M1FakeError> {
        let mut encoded = BytesMut::new();
        encode_command(&mut encoded, command).expect("client command encodes");
        cluster.handle_bytes(connection, &mut encoded.freeze())
    }

    fn take_frames(cluster: &mut M1FakeCluster, connection: ConnectionId) -> Vec<Frame> {
        cluster
            .take_output(connection)
            .expect("connection remains open")
            .into_iter()
            .map(|mut bytes| {
                let frame = decode_one(&mut bytes).expect("fake output decodes");
                assert!(
                    bytes.is_empty(),
                    "one fake output contains exactly one frame"
                );
                frame
            })
            .collect()
    }

    fn connect(cluster: &mut M1FakeCluster, endpoint: Endpoint) -> ConnectionId {
        let connection = cluster
            .open_connection(endpoint)
            .expect("fixture endpoint exists");
        send(cluster, connection, &connect_command(None, None)).expect("CONNECT accepted");
        let frames = take_frames(cluster, connection);
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].command.r#type,
            pb::base_command::Type::Connected as i32
        );
        connection
    }

    fn connect_command(auth_method: Option<&str>, auth_data: Option<Bytes>) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Connect as i32,
            connect: Some(pb::CommandConnect {
                client_version: "m1-fake-self-test".to_owned(),
                auth_method_name: auth_method.map(str::to_owned),
                auth_data,
                feature_flags: Some(pb::FeatureFlags {
                    supports_scalable_topics: Some(true),
                    ..pb::FeatureFlags::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn lookup_command(topic: &str, request_id: u64) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Lookup as i32,
            lookup_topic: Some(pb::CommandLookupTopic {
                topic: topic.to_owned(),
                request_id,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn segment_subscribe_command(
        topic: &str,
        subscription: &str,
        controller_consumer_name: &str,
        segment_id: u64,
        consumer_id: u64,
        request_id: u64,
        sub_type: pb::command_subscribe::SubType,
    ) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Subscribe as i32,
            subscribe: Some(pb::CommandSubscribe {
                topic: topic.to_owned(),
                subscription: subscription.to_owned(),
                sub_type: sub_type as i32,
                consumer_id,
                request_id,
                consumer_name: Some(format!("{controller_consumer_name}-seg-{segment_id}")),
                initial_position: Some(pb::command_subscribe::InitialPosition::Earliest as i32),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn flow_command(consumer_id: u64, permits: u32) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Flow as i32,
            flow: Some(pb::CommandFlow {
                consumer_id,
                message_permits: permits,
            }),
            ..Default::default()
        }
    }

    fn ack_command(
        consumer_id: u64,
        request_id: u64,
        message_id: pb::MessageIdData,
    ) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Ack as i32,
            ack: Some(pb::CommandAck {
                consumer_id,
                ack_type: pb::command_ack::AckType::Individual as i32,
                message_id: vec![message_id],
                request_id: Some(request_id),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn tc_connect_command(request_id: u64) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::TcClientConnectRequest as i32,
            tc_client_connect_request: Some(pb::CommandTcClientConnectRequest {
                request_id,
                tc_id: 0,
                scalable: None,
            }),
            ..Default::default()
        }
    }

    fn new_transaction_command(request_id: u64) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::NewTxn as i32,
            new_txn: Some(pb::CommandNewTxn {
                request_id,
                txn_ttl_millis: Some(30_000),
                tc_id: Some(0),
                scalable: None,
            }),
            ..Default::default()
        }
    }

    fn add_subscription_to_transaction_command(
        request_id: u64,
        txn_id: magnetar_proto::TxnId,
        topic: &str,
        subscription: &str,
    ) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::AddSubscriptionToTxn as i32,
            add_subscription_to_txn: Some(pb::CommandAddSubscriptionToTxn {
                request_id,
                txnid_least_bits: Some(txn_id.least_sig_bits),
                txnid_most_bits: Some(txn_id.most_sig_bits),
                subscription: vec![pb::Subscription {
                    topic: topic.to_owned(),
                    subscription: subscription.to_owned(),
                }],
                scalable: None,
            }),
            ..Default::default()
        }
    }

    fn transactional_ack_command(
        consumer_id: u64,
        request_id: u64,
        txn_id: magnetar_proto::TxnId,
        message_id: pb::MessageIdData,
    ) -> pb::BaseCommand {
        let mut command = ack_command(consumer_id, request_id, message_id);
        let ack = command.ack.as_mut().expect("CommandAck");
        ack.txnid_least_bits = Some(txn_id.least_sig_bits);
        ack.txnid_most_bits = Some(txn_id.most_sig_bits);
        command
    }

    fn end_transaction_command(
        request_id: u64,
        txn_id: magnetar_proto::TxnId,
        action: pb::TxnAction,
    ) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::EndTxn as i32,
            end_txn: Some(pb::CommandEndTxn {
                request_id,
                txnid_least_bits: Some(txn_id.least_sig_bits),
                txnid_most_bits: Some(txn_id.most_sig_bits),
                txn_action: Some(action as i32),
                scalable: None,
            }),
            ..Default::default()
        }
    }

    fn redeliver_command(consumer_id: u64, message_id: pb::MessageIdData) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::RedeliverUnacknowledgedMessages as i32,
            redeliver_unacknowledged_messages: Some(pb::CommandRedeliverUnacknowledgedMessages {
                consumer_id,
                message_ids: vec![message_id],
                consumer_epoch: None,
            }),
            ..Default::default()
        }
    }

    fn redeliver_all_command(consumer_id: u64) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::RedeliverUnacknowledgedMessages as i32,
            redeliver_unacknowledged_messages: Some(pb::CommandRedeliverUnacknowledgedMessages {
                consumer_id,
                message_ids: Vec::new(),
                consumer_epoch: None,
            }),
            ..Default::default()
        }
    }

    fn seek_command(
        consumer_id: u64,
        request_id: u64,
        message_id: pb::MessageIdData,
    ) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Seek as i32,
            seek: Some(pb::CommandSeek {
                consumer_id,
                request_id,
                message_id: Some(message_id),
                message_publish_time: None,
            }),
            ..Default::default()
        }
    }

    fn close_command(consumer_id: u64, request_id: u64) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::CloseConsumer as i32,
            close_consumer: Some(pb::CommandCloseConsumer {
                consumer_id,
                request_id,
                assigned_broker_service_url: None,
                assigned_broker_service_url_tls: None,
            }),
            ..Default::default()
        }
    }

    fn scalable_lookup_command(session_id: u64, topic: &str) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::ScalableTopicLookup as i32,
            scalable_topic_lookup: Some(pb::CommandScalableTopicLookup {
                session_id,
                topic: topic.to_owned(),
            }),
            ..Default::default()
        }
    }

    fn scalable_subscribe_command(
        topic: &str,
        subscription: &str,
        consumer_name: &str,
        consumer_id: u64,
        request_id: u64,
    ) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::ScalableTopicSubscribe as i32,
            scalable_topic_subscribe: Some(pb::CommandScalableTopicSubscribe {
                request_id,
                topic: topic.to_owned(),
                subscription: subscription.to_owned(),
                consumer_name: consumer_name.to_owned(),
                consumer_id,
                consumer_type: pb::ScalableConsumerType::Stream as i32,
            }),
            ..Default::default()
        }
    }

    fn split_layout(epoch: u64) -> Vec<M1Segment> {
        vec![
            M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
                .with_children([3, 4])
                .sealed_at(epoch),
            M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), epoch).with_parents([1]),
            M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(1), epoch).with_parents([1]),
            M1Segment::active(2, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 0),
        ]
    }

    fn register_member(
        cluster: &mut M1FakeCluster,
        controller: ConnectionId,
        subscription: &str,
        consumer_name: &str,
        consumer_id: u64,
        request_id: u64,
    ) -> MemberId {
        let topic = cluster.topic().to_owned();
        send(
            cluster,
            controller,
            &scalable_subscribe_command(
                &topic,
                subscription,
                consumer_name,
                consumer_id,
                request_id,
            ),
        )
        .expect("controller member opens");
        let frames = take_frames(cluster, controller);
        assert_eq!(frames.len(), 1, "controller member receives one response");
        assert!(
            frames[0]
                .command
                .scalable_topic_subscribe_response
                .as_ref()
                .is_some_and(|response| response.error.is_none())
        );
        MemberId::new(controller, consumer_id)
    }

    fn complete_empty_segment(
        cluster: &mut M1FakeCluster,
        subscription: &str,
        consumer_name: &str,
        segment_id: u64,
        consumer_id: u64,
        request_id: u64,
    ) -> ConnectionId {
        cluster
            .terminate_segment(segment_id)
            .expect("segment terminates");
        let endpoint = cluster
            .segment_endpoint(segment_id)
            .expect("draining segment retains a serving route");
        let connection = connect(cluster, endpoint);
        let topic = cluster.segment_topic(segment_id).expect("segment topic");
        send(
            cluster,
            connection,
            &segment_subscribe_command(
                &topic,
                subscription,
                consumer_name,
                segment_id,
                consumer_id,
                request_id,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("terminal segment child opens");
        let frames = take_frames(cluster, connection);
        assert_eq!(frames.len(), 2);
        assert!(frames[0].command.success.is_some());
        assert!(frames[1].command.reached_end_of_topic.is_some());
        assert!(cluster.segment_is_complete(subscription, segment_id));
        connection
    }

    fn message_frame(frames: &[Frame]) -> &Frame {
        frames
            .iter()
            .find(|frame| frame.command.message.is_some())
            .expect("a message frame was emitted")
    }

    #[test]
    fn standard_fixture_retains_complete_history_roots() {
        let mut cluster = M1FakeCluster::two_segment();
        assert!(
            cluster
                .current_layout
                .segments
                .values()
                .all(|segment| segment.created_at_epoch == 0)
        );

        let retained = cluster.current_layout.segments.values().cloned().collect();
        cluster
            .advance_layout(2, retained)
            .expect("epoch-zero roots remain valid in later snapshots");
    }

    #[test]
    fn transport_authorities_and_authentication_are_explicit_and_redacted() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let validator_calls = Arc::clone(&calls);
        let config = M1FakeConfig::new("topic://public/default/secured")
            .expect("topic is valid")
            .with_endpoint_authorities(
                Endpoint::Controller,
                EndpointAuthorities::new(
                    "pulsar://controller.custom:7000",
                    "pulsar+ssl://controller.custom:7443",
                ),
            )
            .with_endpoint_authorities(
                Endpoint::Segment(1),
                EndpointAuthorities::new(
                    "pulsar://segment.custom:7001",
                    "pulsar+ssl://segment.custom:7444",
                ),
            )
            .with_auth_validator(move |attempt| {
                validator_calls.fetch_add(1, Ordering::SeqCst);
                let rendered = format!("{attempt:?}");
                assert!(!rendered.contains("token"));
                assert!(!rendered.contains("accepted"));
                attempt.endpoint == Endpoint::Controller
                    && attempt.transport == TransportSecurity::Tls
                    && attempt.method == Some("token")
                    && attempt.data == Some(b"accepted".as_slice())
            });
        let mut cluster = M1FakeCluster::from_config(config).expect("configuration is valid");

        let rejected = cluster
            .open_connection(Endpoint::Controller)
            .expect("plaintext endpoint exists");
        let error = send(
            &mut cluster,
            rejected,
            &connect_command(
                Some("token"),
                Some(Bytes::from_static(b"credential-must-not-appear")),
            ),
        )
        .expect_err("validator rejects plaintext and the wrong credential");
        assert!(matches!(
            error,
            M1FakeError::AuthenticationRejected {
                endpoint: Endpoint::Controller
            }
        ));
        let diagnostics = format!("{error:?} {cluster:?}");
        assert!(!diagnostics.contains("credential-must-not-appear"));
        assert!(take_frames(&mut cluster, rejected).is_empty());

        let controller = cluster
            .open_connection_with_transport(Endpoint::Controller, TransportSecurity::Tls)
            .expect("TLS endpoint exists");
        send(
            &mut cluster,
            controller,
            &connect_command(Some("token"), Some(Bytes::from_static(b"accepted"))),
        )
        .expect("validator accepts the TLS credential");
        assert_eq!(take_frames(&mut cluster, controller).len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let segment_topic = cluster.segment_topic(1).expect("segment attachment");
        send(&mut cluster, controller, &lookup_command(&segment_topic, 9))
            .expect("direct lookup succeeds");
        let lookup = take_frames(&mut cluster, controller)[0]
            .command
            .lookup_topic_response
            .clone()
            .expect("LookupResponse");
        assert_eq!(
            lookup.broker_service_url.as_deref(),
            Some("pulsar://segment.custom:7001")
        );
        assert_eq!(
            lookup.broker_service_url_tls.as_deref(),
            Some("pulsar+ssl://segment.custom:7444")
        );

        let topic = cluster.topic().to_owned();
        send(
            &mut cluster,
            controller,
            &scalable_lookup_command(10, &topic),
        )
        .expect("layout lookup succeeds");
        let frames = take_frames(&mut cluster, controller);
        let dag = frames[0]
            .command
            .scalable_topic_update
            .as_ref()
            .and_then(|update| update.dag.as_ref())
            .expect("full DAG");
        assert_eq!(
            dag.controller_broker_url.as_deref(),
            Some("pulsar://controller.custom:7000")
        );
        assert_eq!(
            dag.controller_broker_url_tls.as_deref(),
            Some("pulsar+ssl://controller.custom:7443")
        );
        let segment = dag
            .segment_brokers
            .iter()
            .find(|address| address.segment_id == 1)
            .expect("segment 1 placement");
        assert_eq!(segment.broker_url, "pulsar://segment.custom:7001");
        assert_eq!(
            segment.broker_url_tls.as_deref(),
            Some("pulsar+ssl://segment.custom:7444")
        );

        let malformed = M1FakeConfig::new("topic://public/default/invalid")
            .expect("topic is valid")
            .with_endpoint_authorities(
                Endpoint::Controller,
                EndpointAuthorities::new(
                    "pulsar+ssl://wrong-scheme:6650",
                    "pulsar+ssl://controller:6651",
                ),
            );
        assert!(matches!(
            M1FakeCluster::from_config(malformed),
            Err(M1FakeError::InvalidLayout(_))
        ));
    }

    #[test]
    fn complete_dag_validation_is_atomic_acyclic_and_bounded() {
        let mut cluster = M1FakeCluster::two_segment();
        let incomplete = vec![
            M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), 2).with_parents([1]),
            M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(1), 2).with_parents([1]),
            M1Segment::active(2, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 0),
        ];
        assert!(matches!(
            cluster.advance_layout(2, incomplete),
            Err(M1FakeError::InvalidLayout(_))
        ));
        assert_eq!(cluster.layout_epoch(), 1);
        assert!(cluster.segment_topic(3).is_none());

        let cycle = vec![
            M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0),
            M1Segment::active(2, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 0),
            M1Segment::active(3, 0, 0, Endpoint::Segment(1), 2)
                .with_parents([4])
                .with_children([4])
                .sealed_at(2),
            M1Segment::active(4, 0, 0, Endpoint::Segment(1), 2)
                .with_parents([3])
                .with_children([3])
                .sealed_at(2),
        ];
        assert!(matches!(
            cluster.advance_layout(2, cycle),
            Err(M1FakeError::InvalidLayout(_))
        ));
        assert_eq!(cluster.layout_epoch(), 1);

        let orphan = vec![
            M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0).sealed_at(2),
            M1Segment::active(2, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 0),
            M1Segment::active(3, 0, 32_767, Endpoint::Segment(1), 1),
        ];
        assert!(matches!(
            cluster.advance_layout(2, orphan),
            Err(M1FakeError::InvalidLayout(_))
        ));

        let mut deep = BTreeMap::new();
        for id in 1..=u64::try_from(MAX_DAG_ANCESTRY_DEPTH + 2).expect("depth fits u64") {
            let mut segment = M1Segment::active(id, 0, HASH_RANGE_END, Endpoint::Segment(1), 1);
            if id > 1 {
                segment.parent_ids.push(id - 1);
            }
            if id <= u64::try_from(MAX_DAG_ANCESTRY_DEPTH + 1).expect("depth fits u64") {
                segment.child_ids.push(id + 1);
            }
            deep.insert(id, segment);
        }
        assert!(matches!(
            validate_acyclic(&deep),
            Err(M1FakeError::InvalidLayout(_))
        ));

        cluster
            .advance_layout(2, split_layout(2))
            .expect("a complete reciprocal split is accepted after rejected snapshots");
        assert_eq!(cluster.layout_epoch(), 2);
        assert_eq!(
            cluster.segment_topic(3).as_deref(),
            Some("segment://public/default/scaled/0000-3fff-3")
        );
    }

    #[test]
    fn layout_gc_drops_only_sealed_nodes_and_rewrites_their_adjacent_edges() {
        let mut cluster = M1FakeCluster::two_segment();
        cluster
            .advance_layout(2, split_layout(2))
            .expect("segment 1 splits");
        cluster
            .advance_layout(
                3,
                vec![
                    M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), 2),
                    M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(1), 2),
                    M1Segment::active(2, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 0),
                ],
            )
            .expect("sealed parent GC permits children to drop its adjacent edge");
        assert_eq!(cluster.layout_epoch(), 3);
        assert_eq!(cluster.segment_endpoint(1), None);
        assert_eq!(
            cluster
                .current_layout
                .segments
                .get(&3)
                .expect("segment 3 remains")
                .parent_ids,
            Vec::<u64>::new()
        );

        let backdated = vec![
            M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), 2),
            M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(1), 2),
            M1Segment::active(2, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 0),
            M1Segment::active(5, 0, 0, Endpoint::Segment(1), 3).sealed_at(3),
        ];
        assert!(matches!(
            cluster.advance_layout(4, backdated),
            Err(M1FakeError::InvalidLayout(_))
        ));
        assert_eq!(cluster.layout_epoch(), 3);
        let reintroduced = vec![
            M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
                .with_children([3, 4])
                .sealed_at(2),
            M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), 2),
            M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(1), 2),
            M1Segment::active(2, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 0),
        ];
        assert!(matches!(
            cluster.advance_layout(4, reintroduced),
            Err(M1FakeError::InvalidLayout(_))
        ));

        let mut active_gc = M1FakeCluster::two_segment();
        assert!(matches!(
            active_gc.advance_layout(
                2,
                vec![
                    M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0),
                    M1Segment::active(3, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 2),
                ],
            ),
            Err(M1FakeError::InvalidLayout(_))
        ));

        let mut undrained_gc = M1FakeCluster::two_segment();
        undrained_gc
            .advance_layout(2, split_layout(2))
            .expect("segment 1 splits before GC");
        let group = GroupKey {
            topic: undrained_gc.topic.clone(),
            subscription: "sub".to_owned(),
        };
        undrained_gc
            .ownership_history
            .insert((group.clone(), 1), MemberId::new(ConnectionId(1), 1));
        let pruned = vec![
            M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), 2),
            M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(1), 2),
            M1Segment::active(2, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 0),
        ];
        assert!(matches!(
            undrained_gc.advance_layout(3, pruned.clone()),
            Err(M1FakeError::InvalidLayout(_))
        ));
        undrained_gc.completed_segments.insert((group, 1));
        undrained_gc
            .advance_layout(3, pruned)
            .expect("confirmed group completion permits sealed-node GC");
    }

    #[test]
    fn failed_pending_owner_cannot_hide_an_undrained_durable_group_from_gc() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let topic = cluster.topic().to_owned();
        cluster
            .script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Delay,
            )
            .expect("pending owner delay scripted");
        send(
            &mut cluster,
            controller,
            &scalable_subscribe_command(&topic, "sub", "pending", 1, 1),
        )
        .expect("pending owner reserves the initial segments");
        register_member(&mut cluster, controller, "sub", "durable", 2, 2);
        let pending = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(
                pending,
                PendingCompletion::Fail(BrokerFailure::new(
                    pb::ServerError::ServiceNotReady,
                    "pending owner failed",
                )),
            )
            .expect("pending registration fails");
        let _ = take_frames(&mut cluster, controller);
        assert!(cluster.ownership_history.is_empty());

        cluster
            .advance_layout(2, split_layout(2))
            .expect("segment 1 splits");
        let pruned = vec![
            M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), 2),
            M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(1), 2),
            M1Segment::active(2, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 0),
        ];
        assert!(matches!(
            cluster.advance_layout(3, pruned),
            Err(M1FakeError::InvalidLayout(_))
        ));
    }

    #[test]
    fn child_open_requires_controller_ownership_and_canonical_identity() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let child = connect(&mut cluster, Endpoint::Segment(1));
        let topic = cluster.segment_topic(1).expect("segment topic");
        let command = segment_subscribe_command(
            &topic,
            "sub",
            "member",
            1,
            7,
            1,
            pb::command_subscribe::SubType::Exclusive,
        );
        assert!(matches!(
            send(&mut cluster, child, &command),
            Err(M1FakeError::InvalidCommand { .. })
        ));

        register_member(&mut cluster, controller, "sub", "member", 100, 2);
        let mut wrong_name = command.clone();
        wrong_name
            .subscribe
            .as_mut()
            .expect("CommandSubscribe")
            .consumer_name = Some("forged-child".to_owned());
        assert!(matches!(
            send(&mut cluster, child, &wrong_name),
            Err(M1FakeError::InvalidCommand { .. })
        ));

        let mut wrong_group = command.clone();
        wrong_group
            .subscribe
            .as_mut()
            .expect("CommandSubscribe")
            .subscription = "other-sub".to_owned();
        assert!(matches!(
            send(&mut cluster, child, &wrong_group),
            Err(M1FakeError::InvalidCommand { .. })
        ));

        let mut noncanonical = command.clone();
        noncanonical
            .subscribe
            .as_mut()
            .expect("CommandSubscribe")
            .topic = "segment://public/default/scaled/0000-7FFF-1".to_owned();
        assert!(matches!(
            send(&mut cluster, child, &noncanonical),
            Err(M1FakeError::InvalidCommand { .. })
        ));

        send(&mut cluster, child, &command).expect("owned canonical child opens");
        assert!(
            take_frames(&mut cluster, child)[0]
                .command
                .success
                .is_some()
        );
    }

    #[test]
    fn existing_children_keep_their_serving_endpoint_across_placement_changes() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let controller_member = register_member(&mut cluster, controller, "sub", "member", 100, 1);
        let old_connection = connect(&mut cluster, Endpoint::Segment(1));
        let topic = cluster.segment_topic(1).expect("segment topic");
        let subscribe = segment_subscribe_command(
            &topic,
            "sub",
            "member",
            1,
            7,
            2,
            pb::command_subscribe::SubType::Exclusive,
        );
        send(&mut cluster, old_connection, &subscribe).expect("child opens on original placement");
        let _ = take_frames(&mut cluster, old_connection);

        cluster
            .advance_layout(
                2,
                vec![
                    M1Segment::active(1, 0, 32_767, Endpoint::Segment(2), 0),
                    M1Segment::active(2, 32_768, HASH_RANGE_END, Endpoint::Segment(2), 0),
                ],
            )
            .expect("placement-only update is valid");
        cluster
            .publish_assignment_plan(2, vec![FullAssignment::new(controller_member, [1, 2])])
            .expect("assignment catches up to placement epoch");
        let _ = take_frames(&mut cluster, controller);
        cluster
            .enqueue_message(1, Bytes::from_static(b"migrated"))
            .expect("message enqueued");
        send(&mut cluster, old_connection, &flow_command(7, 1))
            .expect("existing child still flows through captured endpoint");
        assert_eq!(
            message_frame(&take_frames(&mut cluster, old_connection))
                .payload
                .as_ref()
                .expect("payload")
                .body,
            Bytes::from_static(b"migrated")
        );
        send(&mut cluster, old_connection, &close_command(7, 3))
            .expect("existing child closes through captured endpoint");
        let _ = take_frames(&mut cluster, old_connection);

        assert!(matches!(
            send(&mut cluster, old_connection, &subscribe),
            Err(M1FakeError::WrongEndpoint {
                expected: Endpoint::Segment(2),
                actual: Endpoint::Segment(1),
                ..
            })
        ));
        let new_connection = connect(&mut cluster, Endpoint::Segment(2));
        send(&mut cluster, new_connection, &subscribe).expect("replacement uses new placement");
        assert!(
            take_frames(&mut cluster, new_connection)[0]
                .command
                .success
                .is_some()
        );
    }

    #[test]
    fn generated_dag_and_assignment_project_into_public_model_types() {
        let mut cluster = M1FakeCluster::two_segment();
        cluster
            .advance_layout(2, split_layout(2))
            .expect("segment 1 splits");
        let dag = cluster.dag_snapshot().expect("generated DAG validates");
        assert_eq!(dag.epoch(), 2);
        assert_eq!(dag.segments().len(), 4);

        let assignment = cluster
            .consumer_assignment(2, [1, 2, 3, 4])
            .expect("generated assignment validates");
        assert_eq!(assignment.layout_epoch(), 2);
        assert_eq!(
            assignment
                .segments()
                .iter()
                .map(|segment| segment.segment_id().0)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn sealed_placements_are_hidden_while_bound_children_keep_their_route() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        register_member(&mut cluster, controller, "sub", "member", 100, 1);
        let topic = cluster.topic().to_owned();
        send(
            &mut cluster,
            controller,
            &scalable_lookup_command(9, &topic),
        )
        .expect("layout watch opens");
        let _ = take_frames(&mut cluster, controller);
        cluster
            .enqueue_message(1, Bytes::from_static(b"pre-seal backlog"))
            .expect("message enqueued before seal");
        let segment_topic = cluster.segment_topic(1).expect("segment topic");
        let child = connect(&mut cluster, Endpoint::Segment(1));
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &segment_topic,
                "sub",
                "member",
                1,
                7,
                2,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("child binds before seal");
        let _ = take_frames(&mut cluster, child);

        cluster
            .advance_layout(2, split_layout(2))
            .expect("segment 1 seals");
        let update = take_frames(&mut cluster, controller);
        let advertised: Vec<_> = update[0]
            .command
            .scalable_topic_update
            .as_ref()
            .and_then(|update| update.dag.as_ref())
            .expect("DAG update")
            .segment_brokers
            .iter()
            .map(|address| address.segment_id)
            .collect();
        assert_eq!(advertised, vec![2, 3, 4]);
        assert_eq!(cluster.segment_endpoint(1), Some(Endpoint::Segment(1)));

        send(&mut cluster, child, &flow_command(7, 1))
            .expect("bound draining generation keeps its captured serving route");
        assert_eq!(
            message_frame(&take_frames(&mut cluster, child))
                .payload
                .as_ref()
                .expect("payload")
                .body,
            Bytes::from_static(b"pre-seal backlog")
        );
    }

    #[test]
    fn sealed_descriptor_rejects_appends_before_explicit_eot() {
        let mut cluster = M1FakeCluster::two_segment();
        cluster
            .advance_layout(2, split_layout(2))
            .expect("segment 1 seals");
        assert!(matches!(
            cluster.enqueue_message(1, Bytes::from_static(b"too late")),
            Err(M1FakeError::InvalidCommand { .. })
        ));
        cluster
            .enqueue_message(3, Bytes::from_static(b"active child"))
            .expect("active child still accepts appends");
    }

    #[test]
    fn terminal_is_emitted_only_after_backlog_dispatch_including_late_opens() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        register_member(&mut cluster, controller, "sub", "member", 100, 1);
        cluster
            .enqueue_message(1, Bytes::from_static(b"first"))
            .expect("first message enqueued");
        cluster
            .enqueue_message(1, Bytes::from_static(b"second"))
            .expect("second message enqueued");
        cluster.terminate_segment(1).expect("segment terminates");

        let topic = cluster.segment_topic(1).expect("segment topic");
        let child = connect(&mut cluster, Endpoint::Segment(1));
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                7,
                2,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("late child opens");
        let opened = take_frames(&mut cluster, child);
        assert_eq!(opened.len(), 1);
        assert!(opened[0].command.success.is_some());

        send(&mut cluster, child, &flow_command(7, 1)).expect("first FLOW accepted");
        let first = take_frames(&mut cluster, child);
        assert_eq!(first.len(), 1, "terminal waits behind retained backlog");
        let first_id = message_frame(&first)
            .command
            .message
            .as_ref()
            .expect("first CommandMessage")
            .message_id
            .clone();

        send(&mut cluster, child, &flow_command(7, 1)).expect("second FLOW accepted");
        let second = take_frames(&mut cluster, child);
        assert_eq!(second.len(), 2);
        assert!(second[0].command.message.is_some());
        assert!(second[1].command.reached_end_of_topic.is_some());
        let second_id = second[0]
            .command
            .message
            .as_ref()
            .expect("second CommandMessage")
            .message_id
            .clone();

        send(&mut cluster, child, &ack_command(7, 3, first_id)).expect("first ACK accepted");
        let _ = take_frames(&mut cluster, child);
        send(&mut cluster, child, &ack_command(7, 4, second_id)).expect("second ACK accepted");
        let _ = take_frames(&mut cluster, child);
        assert!(cluster.segment_is_complete("sub", 1));
        send(&mut cluster, child, &close_command(7, 5)).expect("first child closes");
        let _ = take_frames(&mut cluster, child);

        let reopened = connect(&mut cluster, Endpoint::Segment(1));
        send(
            &mut cluster,
            reopened,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                8,
                6,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("post-terminal child opens");
        let post_terminal = take_frames(&mut cluster, reopened);
        assert_eq!(post_terminal.len(), 2);
        assert!(post_terminal[0].command.success.is_some());
        assert!(post_terminal[1].command.reached_end_of_topic.is_some());
    }

    #[test]
    fn ack_rechecks_eot_after_removing_queued_redelivery() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        register_member(&mut cluster, controller, "sub", "member", 100, 1);
        let topic = cluster.segment_topic(1).expect("segment topic");
        let child = connect(&mut cluster, Endpoint::Segment(1));
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                7,
                2,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("child opens");
        let _ = take_frames(&mut cluster, child);
        cluster
            .enqueue_message(1, Bytes::from_static(b"redelivery"))
            .expect("message enqueued");
        send(&mut cluster, child, &flow_command(7, 1)).expect("message requested");
        let delivered = take_frames(&mut cluster, child);
        let delivered_id = message_frame(&delivered)
            .command
            .message
            .as_ref()
            .expect("CommandMessage")
            .message_id
            .clone();
        send(
            &mut cluster,
            child,
            &redeliver_command(7, delivered_id.clone()),
        )
        .expect("redelivery queues without a permit");
        cluster.terminate_segment(1).expect("segment terminates");
        assert!(take_frames(&mut cluster, child).is_empty());

        send(&mut cluster, child, &ack_command(7, 3, delivered_id))
            .expect("ACK removes both unacked and queued redelivery state");
        let settled = take_frames(&mut cluster, child);
        assert_eq!(settled.len(), 2);
        assert!(settled[0].command.ack_response.is_some());
        assert!(settled[1].command.reached_end_of_topic.is_some());
        assert!(cluster.segment_is_complete("sub", 1));
    }

    #[test]
    fn transaction_coordinator_stages_commit_and_abort_cursor_effects() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        register_member(&mut cluster, controller, "sub", "member", 100, 1);
        let topic = cluster.segment_topic(1).expect("segment topic");
        let child = connect(&mut cluster, Endpoint::Segment(1));
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                7,
                2,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("child opens");
        let _ = take_frames(&mut cluster, child);

        send(
            &mut cluster,
            controller,
            &lookup_command(TRANSACTION_COORDINATOR_TOPIC, 10),
        )
        .expect("coordinator lookup is routed by the fake");
        assert!(
            take_frames(&mut cluster, controller)[0]
                .command
                .lookup_topic_response
                .is_some()
        );
        send(&mut cluster, controller, &tc_connect_command(11))
            .expect("coordinator handshake accepted");
        assert!(
            take_frames(&mut cluster, controller)[0]
                .command
                .tc_client_connect_response
                .is_some()
        );
        send(&mut cluster, controller, &new_transaction_command(12))
            .expect("transaction allocated");
        let opened = take_frames(&mut cluster, controller)[0]
            .command
            .new_txn_response
            .clone()
            .expect("NewTxnResponse");
        let commit_txn = magnetar_proto::TxnId::new(
            opened.txnid_most_bits.expect("most bits"),
            opened.txnid_least_bits.expect("least bits"),
        );
        send(
            &mut cluster,
            controller,
            &add_subscription_to_transaction_command(13, commit_txn, &topic, "sub"),
        )
        .expect("subscription registration accepted");
        assert!(
            take_frames(&mut cluster, controller)[0]
                .command
                .add_subscription_to_txn_response
                .is_some()
        );

        cluster
            .enqueue_message(1, Bytes::from_static(b"transactional"))
            .expect("message enqueued");
        send(&mut cluster, child, &flow_command(7, 1)).expect("message requested");
        let delivered = take_frames(&mut cluster, child);
        let delivered_id = message_frame(&delivered)
            .command
            .message
            .as_ref()
            .expect("CommandMessage")
            .message_id
            .clone();
        send(
            &mut cluster,
            child,
            &transactional_ack_command(7, 14, commit_txn, delivered_id),
        )
        .expect("registered transactional ACK is staged");
        assert!(
            take_frames(&mut cluster, child)[0]
                .command
                .ack_response
                .is_some()
        );
        assert_eq!(cluster.resource_counts().unacked_messages, 1);
        assert_eq!(cluster.durable_cursor("sub", 1), Some(0));
        assert_eq!(
            cluster
                .transaction_observation(commit_txn)
                .expect("commit transaction")
                .staged_acknowledgements,
            1
        );

        send(
            &mut cluster,
            controller,
            &end_transaction_command(15, commit_txn, pb::TxnAction::Commit),
        )
        .expect("commit applies the staged ACK");
        assert!(
            take_frames(&mut cluster, controller)[0]
                .command
                .end_txn_response
                .is_some()
        );
        assert_eq!(cluster.resource_counts().unacked_messages, 0);
        assert_eq!(cluster.durable_cursor("sub", 1), Some(1));
        let committed = cluster
            .transaction_observation(commit_txn)
            .expect("committed transaction retained for observation");
        assert_eq!(committed.state, FakeTransactionState::Committed);
        assert_eq!(committed.staged_acknowledgements, 0);

        send(&mut cluster, controller, &new_transaction_command(16))
            .expect("abort transaction allocated");
        let opened = take_frames(&mut cluster, controller)[0]
            .command
            .new_txn_response
            .clone()
            .expect("NewTxnResponse");
        let abort_txn = magnetar_proto::TxnId::new(
            opened.txnid_most_bits.expect("most bits"),
            opened.txnid_least_bits.expect("least bits"),
        );
        send(
            &mut cluster,
            controller,
            &add_subscription_to_transaction_command(17, abort_txn, &topic, "sub"),
        )
        .expect("abort subscription registration accepted");
        let _ = take_frames(&mut cluster, controller);
        cluster
            .enqueue_message(1, Bytes::from_static(b"abort-redelivery"))
            .expect("abort message enqueued");
        send(&mut cluster, child, &flow_command(7, 1)).expect("abort message requested");
        let delivered = take_frames(&mut cluster, child);
        let delivered_id = message_frame(&delivered)
            .command
            .message
            .as_ref()
            .expect("CommandMessage")
            .message_id
            .clone();
        send(
            &mut cluster,
            child,
            &transactional_ack_command(7, 18, abort_txn, delivered_id.clone()),
        )
        .expect("abort ACK staged");
        let _ = take_frames(&mut cluster, child);
        send(&mut cluster, child, &flow_command(7, 1)).expect("redelivery permit retained");
        assert!(take_frames(&mut cluster, child).is_empty());
        send(
            &mut cluster,
            controller,
            &end_transaction_command(19, abort_txn, pb::TxnAction::Abort),
        )
        .expect("abort retains the durable cursor and schedules redelivery");
        let _ = take_frames(&mut cluster, controller);
        let redelivered = take_frames(&mut cluster, child);
        let redelivered_message = message_frame(&redelivered)
            .command
            .message
            .as_ref()
            .expect("redelivered CommandMessage");
        assert_eq!(redelivered_message.message_id, delivered_id);
        assert_eq!(redelivered_message.redelivery_count, Some(1));
        assert_eq!(cluster.durable_cursor("sub", 1), Some(1));
        assert_eq!(
            cluster
                .transaction_observation(abort_txn)
                .expect("abort transaction")
                .state,
            FakeTransactionState::Aborted
        );

        send(&mut cluster, child, &ack_command(7, 20, delivered_id))
            .expect("redelivered message remains ordinarily acknowledgeable");
        assert!(
            take_frames(&mut cluster, child)[0]
                .command
                .ack_response
                .is_some()
        );
    }

    #[test]
    fn end_transaction_rejects_delayed_registration_and_ack_work() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        register_member(&mut cluster, controller, "sub", "member", 100, 1);
        let topic = cluster.segment_topic(1).expect("segment topic");
        let child = connect(&mut cluster, Endpoint::Segment(1));
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                7,
                2,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("child opens");
        let _ = take_frames(&mut cluster, child);
        send(&mut cluster, controller, &tc_connect_command(3)).expect("coordinator connects");
        let _ = take_frames(&mut cluster, controller);
        send(&mut cluster, controller, &new_transaction_command(4)).expect("transaction opens");
        let opened = take_frames(&mut cluster, controller)[0]
            .command
            .new_txn_response
            .clone()
            .expect("NewTxnResponse");
        let txn_id = magnetar_proto::TxnId::new(
            opened.txnid_most_bits.expect("most bits"),
            opened.txnid_least_bits.expect("least bits"),
        );

        cluster
            .script_next(
                Endpoint::Controller,
                OperationKind::TransactionRegistration,
                ScriptedBehavior::Delay,
            )
            .expect("registration delay scripted");
        send(
            &mut cluster,
            controller,
            &add_subscription_to_transaction_command(5, txn_id, &topic, "sub"),
        )
        .expect("registration is delayed");
        assert!(matches!(
            send(
                &mut cluster,
                controller,
                &end_transaction_command(6, txn_id, pb::TxnAction::Commit),
            ),
            Err(M1FakeError::InvalidCommand {
                command: pb::base_command::Type::EndTxn,
                ..
            })
        ));
        let registration = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(registration, PendingCompletion::Succeed)
            .expect("registration completes");
        let _ = take_frames(&mut cluster, controller);

        cluster
            .enqueue_message(1, Bytes::from_static(b"pending-ack"))
            .expect("message enqueued");
        send(&mut cluster, child, &flow_command(7, 1)).expect("message requested");
        let delivered = take_frames(&mut cluster, child);
        let delivered_id = message_frame(&delivered)
            .command
            .message
            .as_ref()
            .expect("CommandMessage")
            .message_id
            .clone();
        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Delay,
            )
            .expect("ack delay scripted");
        send(
            &mut cluster,
            child,
            &transactional_ack_command(7, 7, txn_id, delivered_id),
        )
        .expect("transactional ack is delayed");
        assert!(matches!(
            send(
                &mut cluster,
                controller,
                &end_transaction_command(8, txn_id, pb::TxnAction::Commit),
            ),
            Err(M1FakeError::InvalidCommand {
                command: pb::base_command::Type::EndTxn,
                ..
            })
        ));
        let acknowledgement = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(acknowledgement, PendingCompletion::Succeed)
            .expect("transactional ack completes");
        let _ = take_frames(&mut cluster, child);

        send(
            &mut cluster,
            controller,
            &end_transaction_command(9, txn_id, pb::TxnAction::Commit),
        )
        .expect("settled transaction commits");
        assert!(
            take_frames(&mut cluster, controller)[0]
                .command
                .end_txn_response
                .is_some()
        );
        assert_eq!(
            cluster
                .transaction_observation(txn_id)
                .expect("transaction observation")
                .state,
            FakeTransactionState::Committed
        );
    }

    #[test]
    fn flow_rejects_a_child_after_controller_ownership_moves() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller_a = connect(&mut cluster, Endpoint::Controller);
        let member_a = register_member(&mut cluster, controller_a, "sub", "member-a", 100, 1);
        let controller_b = connect(&mut cluster, Endpoint::Controller);
        let member_b = register_member(&mut cluster, controller_b, "sub", "member-b", 101, 2);
        let topic = cluster.segment_topic(1).expect("segment topic");
        let child = connect(&mut cluster, Endpoint::Segment(1));
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member-a",
                1,
                7,
                3,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("old owner child opens");
        let _ = take_frames(&mut cluster, child);

        cluster
            .publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(member_a, [2]),
                    FullAssignment::new(member_b, [1]),
                ],
            )
            .expect("ownership moves");
        assert!(matches!(
            send(&mut cluster, child, &flow_command(7, 1)),
            Err(M1FakeError::InvalidCommand {
                command: pb::base_command::Type::Flow,
                ..
            })
        ));
        assert_eq!(cluster.resource_counts().permits, 0);
    }

    #[test]
    fn broker_flow_does_not_enforce_client_strict_ordering() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let member = register_member(&mut cluster, controller, "sub", "member", 100, 1);
        cluster
            .advance_layout(2, split_layout(2))
            .expect("segment 1 splits");
        let _ = take_frames(&mut cluster, controller);
        let parent = complete_empty_segment(&mut cluster, "sub", "member", 1, 8, 2);
        cluster
            .publish_assignment_plan(2, vec![FullAssignment::new(member, [1, 2, 3, 4])])
            .expect("drained parent makes its active children assignable");
        let _ = take_frames(&mut cluster, controller);

        let child_topic = cluster.segment_topic(3).expect("child attachment");
        let child = connect(&mut cluster, Endpoint::Segment(1));
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &child_topic,
                "sub",
                "member",
                3,
                7,
                2,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("eligible child attaches");
        let _ = take_frames(&mut cluster, child);
        cluster
            .enqueue_message(3, Bytes::from_static(b"blocked"))
            .expect("child message enqueued");

        send(&mut cluster, parent, &seek_command(8, 4, message_id(1, 0)))
            .expect("rewinding the parent removes its completion proof");
        let _ = take_frames(&mut cluster, parent);
        assert_eq!(
            cluster
                .drain_eligibility(member, 3)
                .expect("eligibility is observable"),
            DrainEligibility::ParentBlocked {
                segment_ids: vec![1]
            }
        );
        send(&mut cluster, child, &flow_command(7, 1))
            .expect("ordinary broker FLOW is independent of Strict policy");
        assert_eq!(
            message_frame(&take_frames(&mut cluster, child))
                .payload
                .as_ref()
                .expect("payload")
                .body,
            Bytes::from_static(b"blocked")
        );
    }

    #[test]
    fn sparse_individual_ack_survives_reconnect_and_empty_redelivery_means_all() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        register_member(&mut cluster, controller, "sub", "member", 100, 1);
        let topic = cluster.segment_topic(1).expect("segment topic");
        let child = connect(&mut cluster, Endpoint::Segment(1));
        let subscribe = segment_subscribe_command(
            &topic,
            "sub",
            "member",
            1,
            7,
            2,
            pb::command_subscribe::SubType::Exclusive,
        );
        send(&mut cluster, child, &subscribe).expect("child opens");
        let _ = take_frames(&mut cluster, child);
        for payload in [b"zero".as_slice(), b"one".as_slice(), b"two".as_slice()] {
            cluster
                .enqueue_message(1, Bytes::copy_from_slice(payload))
                .expect("message enqueued");
        }
        send(&mut cluster, child, &flow_command(7, 3)).expect("three permits accepted");
        let delivered = take_frames(&mut cluster, child);
        let ids: Vec<_> = delivered
            .iter()
            .filter_map(|frame| {
                frame
                    .command
                    .message
                    .as_ref()
                    .map(|message| message.message_id.clone())
            })
            .collect();
        assert_eq!(ids.len(), 3);
        let mut forged_id = ids[0].clone();
        forged_id.partition = Some(42);
        assert!(matches!(
            send(&mut cluster, child, &ack_command(7, 30, forged_id)),
            Err(M1FakeError::InvalidCommand { .. })
        ));
        send(&mut cluster, child, &ack_command(7, 3, ids[1].clone()))
            .expect("middle message ACK accepted");
        let _ = take_frames(&mut cluster, child);
        cluster
            .disconnect_connection(child)
            .expect("child transport drops");

        let reconnected = connect(&mut cluster, Endpoint::Segment(1));
        send(&mut cluster, reconnected, &subscribe).expect("child reconnects");
        let _ = take_frames(&mut cluster, reconnected);
        send(&mut cluster, reconnected, &flow_command(7, 2)).expect("reconnect FLOW accepted");
        let replay = take_frames(&mut cluster, reconnected);
        let payloads: Vec<_> = replay
            .iter()
            .filter_map(|frame| frame.payload.as_ref().map(|payload| payload.body.clone()))
            .collect();
        assert_eq!(
            payloads,
            vec![Bytes::from_static(b"zero"), Bytes::from_static(b"two")],
            "the durable sparse ACK skips only entry 1"
        );

        send(&mut cluster, reconnected, &redeliver_all_command(7))
            .expect("empty message-id list requests all unacked messages");
        assert!(take_frames(&mut cluster, reconnected).is_empty());
        send(&mut cluster, reconnected, &flow_command(7, 2)).expect("redelivery FLOW accepted");
        let redelivered = take_frames(&mut cluster, reconnected);
        assert_eq!(
            redelivered
                .iter()
                .filter(|frame| {
                    frame
                        .command
                        .message
                        .as_ref()
                        .is_some_and(|message| message.redelivery_count == Some(1))
                })
                .count(),
            2
        );
    }

    #[test]
    fn failed_first_open_leaves_latest_retry_at_the_ledger_head() {
        for delayed in [false, true] {
            let mut cluster = M1FakeCluster::two_segment();
            let controller = connect(&mut cluster, Endpoint::Controller);
            register_member(&mut cluster, controller, "sub", "member", 100, 1);
            cluster
                .enqueue_message(1, Bytes::from_static(b"existing"))
                .expect("message enqueued before first open");
            let topic = cluster.segment_topic(1).expect("segment topic");
            let child = connect(&mut cluster, Endpoint::Segment(1));
            cluster
                .script_next(
                    Endpoint::Segment(1),
                    OperationKind::SegmentOpen,
                    if delayed {
                        ScriptedBehavior::Delay
                    } else {
                        ScriptedBehavior::Fail(BrokerFailure::new(
                            pb::ServerError::ServiceNotReady,
                            "first open failed",
                        ))
                    },
                )
                .expect("first-open behavior scripted");
            send(
                &mut cluster,
                child,
                &segment_subscribe_command(
                    &topic,
                    "sub",
                    "member",
                    1,
                    7,
                    2,
                    pb::command_subscribe::SubType::Exclusive,
                ),
            )
            .expect("first open receives scripted behavior");
            if delayed {
                let pending = cluster.pending_operations()[0].id;
                cluster
                    .complete_pending(
                        pending,
                        PendingCompletion::Fail(BrokerFailure::new(
                            pb::ServerError::ServiceNotReady,
                            "delayed first open failed",
                        )),
                    )
                    .expect("delayed first open fails");
            }
            let _ = take_frames(&mut cluster, child);
            assert_eq!(cluster.resource_counts().child_consumers, 0);

            let mut latest = segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                8,
                3,
                pb::command_subscribe::SubType::Exclusive,
            );
            latest
                .subscribe
                .as_mut()
                .expect("CommandSubscribe")
                .initial_position = Some(pb::command_subscribe::InitialPosition::Latest as i32);
            send(&mut cluster, child, &latest).expect("Latest retry opens");
            let _ = take_frames(&mut cluster, child);
            send(&mut cluster, child, &flow_command(8, 1)).expect("FLOW accepted");
            assert!(
                take_frames(&mut cluster, child).is_empty(),
                "failed first open must not manufacture an earliest durable cursor"
            );
            assert_eq!(cluster.resource_counts().permits, 1);
        }
    }

    #[test]
    fn delayed_open_preserves_cursor_and_stale_ack_cannot_mutate_replacement() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        register_member(&mut cluster, controller, "sub", "member", 100, 1);
        cluster
            .enqueue_message(1, Bytes::from_static(b"retained"))
            .expect("message enqueued");
        let topic = cluster.segment_topic(1).expect("segment topic");
        let child = connect(&mut cluster, Endpoint::Segment(1));
        let mut latest = segment_subscribe_command(
            &topic,
            "sub",
            "member",
            1,
            7,
            2,
            pb::command_subscribe::SubType::Exclusive,
        );
        latest
            .subscribe
            .as_mut()
            .expect("CommandSubscribe")
            .initial_position = Some(pb::command_subscribe::InitialPosition::Latest as i32);
        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::SegmentOpen,
                ScriptedBehavior::Delay,
            )
            .expect("open delay scripted");
        send(&mut cluster, child, &latest).expect("latest open is delayed");
        let open_id = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(
                open_id,
                PendingCompletion::Fail(BrokerFailure::new(
                    pb::ServerError::ServiceNotReady,
                    "delayed latest open failed",
                )),
            )
            .expect("delayed open fails without committing its cursor");
        let _ = take_frames(&mut cluster, child);

        let earliest = segment_subscribe_command(
            &topic,
            "sub",
            "member",
            1,
            7,
            3,
            pb::command_subscribe::SubType::Exclusive,
        );
        send(&mut cluster, child, &earliest).expect("earliest retry opens");
        let _ = take_frames(&mut cluster, child);
        send(&mut cluster, child, &flow_command(7, 1)).expect("retained message requested");
        let delivered = take_frames(&mut cluster, child);
        let delivered_id = message_frame(&delivered)
            .command
            .message
            .as_ref()
            .expect("CommandMessage")
            .message_id
            .clone();

        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Delay,
            )
            .expect("ACK delay scripted");
        send(&mut cluster, child, &ack_command(7, 4, delivered_id)).expect("ACK held");
        let ack_id = cluster.pending_operations()[0].id;
        send(&mut cluster, child, &close_command(7, 5)).expect("old generation closes");
        let _ = take_frames(&mut cluster, child);
        send(&mut cluster, child, &earliest).expect("same wire id opens a new generation");
        let _ = take_frames(&mut cluster, child);

        assert!(matches!(
            cluster.complete_pending(ack_id, PendingCompletion::Succeed),
            Err(M1FakeError::StalePending(id)) if id == ack_id
        ));
        assert_eq!(cluster.resource_counts().child_consumers, 1);
        assert!(take_frames(&mut cluster, child).is_empty());
        send(&mut cluster, child, &flow_command(7, 1))
            .expect("replacement still sees the unacknowledged message");
        assert_eq!(
            message_frame(&take_frames(&mut cluster, child))
                .payload
                .as_ref()
                .expect("payload")
                .body,
            Bytes::from_static(b"retained")
        );
    }

    #[test]
    fn seek_fences_delayed_ack_and_reopens_a_completed_barrier() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        register_member(&mut cluster, controller, "sub", "member", 100, 1);
        let topic = cluster.segment_topic(1).expect("segment topic");
        let child = connect(&mut cluster, Endpoint::Segment(1));
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                7,
                2,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("child opens");
        let _ = take_frames(&mut cluster, child);
        cluster
            .enqueue_message(1, Bytes::from_static(b"seekable"))
            .expect("message enqueued");
        cluster.terminate_segment(1).expect("segment terminates");
        send(&mut cluster, child, &flow_command(7, 1)).expect("message requested");
        let delivered = take_frames(&mut cluster, child);
        let delivered_id = message_frame(&delivered)
            .command
            .message
            .as_ref()
            .expect("CommandMessage")
            .message_id
            .clone();

        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Delay,
            )
            .expect("ACK delay scripted");
        send(
            &mut cluster,
            child,
            &ack_command(7, 3, delivered_id.clone()),
        )
        .expect("ACK held");
        let stale_ack = cluster.pending_operations()[0].id;
        send(
            &mut cluster,
            child,
            &seek_command(7, 4, delivered_id.clone()),
        )
        .expect("seek advances the child generation");
        let _ = take_frames(&mut cluster, child);
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                7,
                40,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("seek replacement re-subscribes");
        let reopened = take_frames(&mut cluster, child);
        assert_eq!(reopened.len(), 1, "EOT resets behind the replay backlog");
        assert!(reopened[0].command.success.is_some());
        assert!(matches!(
            cluster.complete_pending(stale_ack, PendingCompletion::Succeed),
            Err(M1FakeError::StalePending(id)) if id == stale_ack
        ));

        send(&mut cluster, child, &flow_command(7, 1)).expect("seek replay requested");
        let replayed = take_frames(&mut cluster, child);
        let replayed_id = message_frame(&replayed)
            .command
            .message
            .as_ref()
            .expect("replayed CommandMessage")
            .message_id
            .clone();
        send(&mut cluster, child, &ack_command(7, 5, replayed_id))
            .expect("replayed message ACK accepted");
        let _ = take_frames(&mut cluster, child);
        assert!(cluster.segment_is_complete("sub", 1));

        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::Seek,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    pb::ServerError::PersistenceError,
                    "seek failed",
                )),
            )
            .expect("seek failure scripted");
        send(
            &mut cluster,
            child,
            &seek_command(7, 6, delivered_id.clone()),
        )
        .expect("scripted seek returns a broker error");
        assert!(take_frames(&mut cluster, child)[0].command.error.is_some());
        assert!(cluster.segment_is_complete("sub", 1));
        assert_eq!(cluster.resource_counts().child_consumers, 1);

        send(&mut cluster, child, &seek_command(7, 7, delivered_id))
            .expect("completed segment can be rewound");
        let _ = take_frames(&mut cluster, child);
        assert!(
            !cluster.segment_is_complete("sub", 1),
            "seek removes the old completion proof"
        );
    }

    #[test]
    fn stale_open_and_close_completions_release_only_their_own_generation() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let member_a = register_member(&mut cluster, controller, "sub", "member-a", 100, 1);
        let member_b = register_member(&mut cluster, controller, "sub", "member-b", 101, 2);
        let topic = cluster.segment_topic(1).expect("segment topic");
        let child = connect(&mut cluster, Endpoint::Segment(1));
        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::SegmentOpen,
                ScriptedBehavior::Delay,
            )
            .expect("open delay scripted");
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member-a",
                1,
                7,
                3,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("member A child open is held");
        let stale_open = cluster.pending_operations()[0].id;
        cluster
            .publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(member_a, [2]),
                    FullAssignment::new(member_b, [1]),
                ],
            )
            .expect("ownership moves to member B");
        let _ = take_frames(&mut cluster, controller);
        assert!(matches!(
            cluster.complete_pending(
                stale_open,
                PendingCompletion::Fail(BrokerFailure::new(
                    pb::ServerError::ServiceNotReady,
                    "stale open failure",
                )),
            ),
            Err(M1FakeError::StalePending(id)) if id == stale_open
        ));
        assert!(take_frames(&mut cluster, child).is_empty());

        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member-b",
                1,
                8,
                4,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("new owner opens after stale reservation is released");
        let _ = take_frames(&mut cluster, child);
        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::Close,
                ScriptedBehavior::Delay,
            )
            .expect("close delay scripted");
        send(&mut cluster, child, &close_command(8, 5)).expect("close is held");
        let stale_close = cluster.pending_operations()[0].id;
        cluster
            .disconnect_connection(controller)
            .expect("controller incarnation ends");
        assert!(matches!(
            cluster.complete_pending(stale_close, PendingCompletion::Succeed),
            Err(M1FakeError::StalePending(id)) if id == stale_close
        ));
        assert_eq!(cluster.resource_counts().child_consumers, 1);
        send(&mut cluster, child, &close_command(8, 6))
            .expect("stale close completion releases its closing marker");
        assert!(
            take_frames(&mut cluster, child)[0]
                .command
                .success
                .is_some()
        );
        assert_eq!(cluster.resource_counts().child_consumers, 0);
    }

    #[test]
    fn production_connection_adapter_exercises_real_m1_framing_and_decoding() {
        let mut cluster = M1FakeCluster::two_segment();
        let fake_connection = cluster
            .open_connection(Endpoint::Controller)
            .expect("controller endpoint exists");
        let adapter = M1ConnectionAdapter::new(fake_connection);
        let mut client = magnetar_proto::Connection::new(
            magnetar_proto::ConnectionConfig::default(),
            Arc::new(|| std::time::SystemTime::UNIX_EPOCH),
        );
        let now = Instant::now();
        client.begin_handshake().expect("client queues CONNECT");
        let handshake = adapter
            .exchange(&mut cluster, &mut client, now)
            .expect("real CONNECT/CONNECTED exchange succeeds");
        assert!(handshake.client_bytes > 0);
        assert_eq!(handshake.broker_frames, 1);
        assert!(client.is_connected());
        while client.poll_event().is_some() {}

        let topic = cluster.topic().to_owned();
        let session_id = client
            .open_scalable_topic_session(&topic)
            .expect("client queues M1 lookup");
        let layout = adapter
            .exchange(&mut cluster, &mut client, now)
            .expect("real layout frames decode");
        assert_eq!(layout.broker_frames, 2, "M1 duplicate baseline is decoded");
        assert_eq!(
            client
                .dag_snapshot(session_id)
                .expect("production state installed the DAG")
                .len(),
            2
        );

        client
            .scalable_topic_subscribe(
                &topic,
                "sub",
                "production-member",
                44,
                magnetar_proto::ScalableConsumerType::Stream,
                magnetar_proto::ControllerIncarnation(1),
            )
            .expect("client queues scalable subscribe");
        let registration = adapter
            .exchange(&mut cluster, &mut client, now)
            .expect("real assignment frame decodes");
        assert_eq!(registration.broker_frames, 1);
        let assignment = client
            .scalable_consumer_assignment(44)
            .expect("production state installed the assignment");
        assert_eq!(
            assignment.segment_topics(),
            vec![
                "segment://public/default/scaled/0000-7fff-1",
                "segment://public/default/scaled/8000-ffff-2",
            ]
        );
    }

    #[test]
    fn production_seek_re_subscribes_before_restoring_permits_and_eot() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        register_member(&mut cluster, controller, "sub", "member", 100, 1);
        let segment_topic = cluster.segment_topic(1).expect("segment topic");
        let fake_connection = cluster
            .open_connection(Endpoint::Segment(1))
            .expect("segment endpoint exists");
        let adapter = M1ConnectionAdapter::new(fake_connection);
        let mut client = magnetar_proto::Connection::new(
            magnetar_proto::ConnectionConfig::default(),
            Arc::new(|| std::time::SystemTime::UNIX_EPOCH),
        );
        let now = Instant::now();
        client.begin_handshake().expect("client queues CONNECT");
        adapter
            .exchange(&mut cluster, &mut client, now)
            .expect("production handshake succeeds");
        while client.poll_event().is_some() {}

        let handle = client.subscribe(magnetar_proto::SubscribeRequest {
            topic: segment_topic,
            subscription: "sub".to_owned(),
            receiver_queue_size: 2,
            initial_position: pb::command_subscribe::InitialPosition::Earliest,
            consumer_name: Some("member-seg-1".to_owned()),
            ..magnetar_proto::SubscribeRequest::default()
        });
        adapter
            .exchange(&mut cluster, &mut client, now)
            .expect("production subscribe succeeds");
        let _ = client.initial_flow(handle, now);
        adapter
            .exchange(&mut cluster, &mut client, now)
            .expect("initial FLOW reaches the fake");
        cluster
            .enqueue_message(1, Bytes::from_static(b"before-seek"))
            .expect("message enqueued");
        adapter
            .exchange(&mut cluster, &mut client, now)
            .expect("first delivery decodes");
        cluster.terminate_segment(1).expect("segment terminates");
        adapter
            .exchange(&mut cluster, &mut client, now)
            .expect("initial EOT decodes");
        assert_eq!(cluster.resource_counts().permits, 1);

        client.seek(
            handle,
            magnetar_proto::SeekTarget::MessageId(magnetar_proto::MessageId::from_pb(&message_id(
                1, 0,
            ))),
        );
        adapter
            .exchange(&mut cluster, &mut client, now)
            .expect("seek success decodes");
        assert_eq!(cluster.resource_counts().child_consumers, 0);
        assert_eq!(cluster.resource_counts().permits, 0);

        assert!(client.resubscribe_consumer_after_seek(handle).is_some());
        let reopened = adapter
            .exchange(&mut cluster, &mut client, now)
            .expect("production re-subscribe succeeds");
        assert_eq!(reopened.broker_frames, 1, "replay backlog delays fresh EOT");
        assert_eq!(cluster.resource_counts().child_consumers, 1);
        assert_eq!(cluster.resource_counts().permits, 0);

        let _ = client.initial_flow(handle, now);
        let replay = adapter
            .exchange(&mut cluster, &mut client, now)
            .expect("post-subscribe FLOW resumes dispatch");
        assert_eq!(replay.broker_frames, 2, "replay is followed by fresh EOT");
        assert_eq!(cluster.resource_counts().permits, 1);
    }

    #[test]
    fn lookup_routes_to_placement_and_exclusive_owner_is_busy() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let child_a = connect(&mut cluster, Endpoint::Segment(1));
        let child_b = connect(&mut cluster, Endpoint::Segment(1));
        let topic = cluster.segment_topic(1).expect("segment topic");
        register_member(&mut cluster, controller, "sub", "member", 100, 1);

        send(&mut cluster, controller, &lookup_command(&topic, 10)).expect("lookup accepted");
        let lookup = take_frames(&mut cluster, controller)
            .pop()
            .expect("lookup response");
        let response = lookup
            .command
            .lookup_topic_response
            .expect("generated lookup response");
        assert_eq!(
            response.broker_service_url.as_deref(),
            cluster.endpoint_url(Endpoint::Segment(1))
        );
        assert_eq!(
            response.response,
            Some(pb::command_lookup_topic_response::LookupType::Connect as i32)
        );

        send(
            &mut cluster,
            child_a,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                11,
                20,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("first exclusive open accepted");
        assert!(
            take_frames(&mut cluster, child_a)[0]
                .command
                .success
                .is_some()
        );

        send(
            &mut cluster,
            child_b,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                12,
                21,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("competing open receives a broker response");
        let busy = take_frames(&mut cluster, child_b)
            .pop()
            .expect("ConsumerBusy response")
            .command
            .error
            .expect("generated CommandError");
        assert_eq!(busy.error, pb::ServerError::ConsumerBusy as i32);
        assert_eq!(cluster.resource_counts().child_consumers, 1);

        let lookup_route = cluster
            .routes()
            .iter()
            .find(|route| route.command == pb::base_command::Type::Lookup)
            .expect("lookup route observed");
        assert_eq!(lookup_route.endpoint, Endpoint::Controller);
        let subscribe_routes: Vec<_> = cluster
            .routes()
            .iter()
            .filter(|route| route.command == pb::base_command::Type::Subscribe)
            .map(|route| route.endpoint)
            .collect();
        assert_eq!(
            subscribe_routes,
            vec![Endpoint::Segment(1), Endpoint::Segment(1)]
        );
    }

    #[test]
    fn invalid_commands_are_rejected_without_silent_rerouting() {
        let mut cluster = M1FakeCluster::two_segment();
        let unconnected = cluster
            .open_connection(Endpoint::Segment(1))
            .expect("endpoint exists");
        let err = send(&mut cluster, unconnected, &flow_command(1, 1))
            .expect_err("pre-handshake command rejected");
        assert!(matches!(err, M1FakeError::HandshakeRequired { .. }));

        let child = connect(&mut cluster, Endpoint::Segment(1));
        let topic_1 = cluster.segment_topic(1).expect("segment 1 topic");
        let topic_2 = cluster.segment_topic(2).expect("segment 2 topic");
        let err = send(&mut cluster, child, &lookup_command(&topic_1, 1))
            .expect_err("lookup cannot run on child endpoint");
        assert!(matches!(
            err,
            M1FakeError::WrongEndpoint {
                expected: Endpoint::Controller,
                actual: Endpoint::Segment(1),
                ..
            }
        ));
        let err = send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic_2,
                "sub",
                "member",
                2,
                2,
                2,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect_err("segment 2 cannot open on endpoint 1");
        assert!(matches!(
            err,
            M1FakeError::WrongEndpoint {
                expected: Endpoint::Segment(2),
                actual: Endpoint::Segment(1),
                ..
            }
        ));
        let err = send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic_1,
                "sub",
                "member",
                1,
                3,
                3,
                pb::command_subscribe::SubType::Shared,
            ),
        )
        .expect_err("non-Exclusive child open rejected");
        assert!(matches!(err, M1FakeError::InvalidCommand { .. }));
        assert_eq!(cluster.resource_counts().child_consumers, 0);
        assert!(
            cluster
                .routes()
                .iter()
                .any(|route| route.command == pb::base_command::Type::Lookup
                    && route.endpoint == Endpoint::Segment(1)),
            "the attempted destination remains observable even when rejected"
        );
    }

    #[test]
    fn flow_delivery_ack_nack_seek_terminal_and_close_conserve_resources() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        register_member(&mut cluster, controller, "sub", "member", 100, 1);
        let child = connect(&mut cluster, Endpoint::Segment(1));
        let topic = cluster.segment_topic(1).expect("segment topic");
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                7,
                1,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("subscribe accepted");
        let _ = take_frames(&mut cluster, child);

        cluster
            .enqueue_message(1, Bytes::from_static(b"first"))
            .expect("enqueue first");
        cluster
            .enqueue_message(1, Bytes::from_static(b"second"))
            .expect("enqueue second");
        assert!(take_frames(&mut cluster, child).is_empty());

        send(&mut cluster, child, &flow_command(7, 1)).expect("FLOW accepted");
        let first = take_frames(&mut cluster, child);
        let first_message = message_frame(&first);
        assert_eq!(
            first_message.payload.as_ref().expect("payload frame").body,
            Bytes::from_static(b"first")
        );
        let first_id = first_message
            .command
            .message
            .as_ref()
            .expect("CommandMessage")
            .message_id
            .clone();
        assert_eq!(cluster.resource_counts().permits, 0);
        assert_eq!(cluster.resource_counts().unacked_messages, 1);

        send(&mut cluster, child, &redeliver_command(7, first_id.clone())).expect("nack accepted");
        assert!(take_frames(&mut cluster, child).is_empty());
        send(&mut cluster, child, &flow_command(7, 1)).expect("redelivery FLOW accepted");
        let redelivery = take_frames(&mut cluster, child);
        assert_eq!(
            message_frame(&redelivery)
                .command
                .message
                .as_ref()
                .expect("redelivered CommandMessage")
                .redelivery_count,
            Some(1)
        );
        assert_eq!(cluster.resource_counts().unacked_messages, 1);

        send(&mut cluster, child, &ack_command(7, 2, first_id.clone())).expect("ACK accepted");
        let ack = take_frames(&mut cluster, child);
        assert!(ack[0].command.ack_response.is_some());
        assert_eq!(cluster.resource_counts().unacked_messages, 0);

        send(&mut cluster, child, &seek_command(7, 3, first_id)).expect("seek accepted");
        assert!(
            take_frames(&mut cluster, child)[0]
                .command
                .success
                .is_some()
        );
        assert_eq!(cluster.resource_counts().child_consumers, 0);
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                7,
                30,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("production-style post-seek re-subscribe accepted");
        let _ = take_frames(&mut cluster, child);
        send(&mut cluster, child, &flow_command(7, 2)).expect("post-seek FLOW accepted");
        let replayed = take_frames(&mut cluster, child);
        assert_eq!(
            replayed
                .iter()
                .filter(|frame| frame.command.message.is_some())
                .count(),
            2
        );
        assert_eq!(cluster.resource_counts().unacked_messages, 2);

        cluster.terminate_segment(1).expect("segment terminates");
        assert!(
            take_frames(&mut cluster, child)[0]
                .command
                .reached_end_of_topic
                .is_some()
        );
        let err = cluster
            .enqueue_message(1, Bytes::from_static(b"late"))
            .expect_err("terminal segment rejects later appends");
        assert!(matches!(err, M1FakeError::InvalidCommand { .. }));

        send(&mut cluster, child, &close_command(7, 4)).expect("close accepted");
        assert!(
            take_frames(&mut cluster, child)[0]
                .command
                .success
                .is_some()
        );
        let counts = cluster.resource_counts();
        assert_eq!(counts.child_consumers, 0);
        assert_eq!(counts.permits, 0);
        assert_eq!(counts.unacked_messages, 0);
    }

    #[test]
    fn delayed_open_ack_and_close_commit_only_on_success() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        register_member(&mut cluster, controller, "sub", "member", 100, 1);
        let child_a = connect(&mut cluster, Endpoint::Segment(1));
        let child_b = connect(&mut cluster, Endpoint::Segment(1));
        let topic = cluster.segment_topic(1).expect("segment topic");
        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::SegmentOpen,
                ScriptedBehavior::Delay,
            )
            .expect("script accepted");
        send(
            &mut cluster,
            child_a,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                10,
                1,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("open is held");
        assert!(take_frames(&mut cluster, child_a).is_empty());
        assert_eq!(cluster.resource_counts().pending_child_opens, 1);

        send(
            &mut cluster,
            child_b,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                20,
                2,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("reservation produces ConsumerBusy");
        let busy = take_frames(&mut cluster, child_b)[0]
            .command
            .error
            .as_ref()
            .expect("CommandError")
            .error;
        assert_eq!(busy, pb::ServerError::ConsumerBusy as i32);

        let open_id = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(
                open_id,
                PendingCompletion::Fail(BrokerFailure::new(
                    pb::ServerError::ServiceNotReady,
                    "open delayed then failed",
                )),
            )
            .expect("delayed open fails");
        assert!(
            take_frames(&mut cluster, child_a)[0]
                .command
                .error
                .is_some()
        );
        assert_eq!(cluster.resource_counts().pending_child_opens, 0);
        assert_eq!(cluster.resource_counts().child_consumers, 0);

        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::SegmentOpen,
                ScriptedBehavior::Delay,
            )
            .expect("retry delay scripted");
        send(
            &mut cluster,
            child_a,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member",
                1,
                11,
                3,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("retry is held");
        let retry_open_id = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(retry_open_id, PendingCompletion::Succeed)
            .expect("delayed retry opens");
        let _ = take_frames(&mut cluster, child_a);
        cluster
            .enqueue_message(1, Bytes::from_static(b"held"))
            .expect("message enqueued");
        send(&mut cluster, child_a, &flow_command(11, 1)).expect("FLOW accepted");
        let delivered = take_frames(&mut cluster, child_a);
        let delivered_id = message_frame(&delivered)
            .command
            .message
            .as_ref()
            .expect("CommandMessage")
            .message_id
            .clone();

        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Delay,
            )
            .expect("ack delay scripted");
        send(
            &mut cluster,
            child_a,
            &ack_command(11, 4, delivered_id.clone()),
        )
        .expect("ACK held");
        assert!(take_frames(&mut cluster, child_a).is_empty());
        assert_eq!(cluster.resource_counts().unacked_messages, 1);
        let ack_id = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(
                ack_id,
                PendingCompletion::Fail(BrokerFailure::new(
                    pb::ServerError::PersistenceError,
                    "ack failed",
                )),
            )
            .expect("delayed ACK fails");
        let ack_failure = take_frames(&mut cluster, child_a)[0]
            .command
            .ack_response
            .as_ref()
            .expect("AckResponse")
            .error;
        assert_eq!(ack_failure, Some(pb::ServerError::PersistenceError as i32));
        assert_eq!(cluster.resource_counts().unacked_messages, 1);
        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Delay,
            )
            .expect("ACK retry delay scripted");
        send(&mut cluster, child_a, &ack_command(11, 5, delivered_id)).expect("ACK retry held");
        let retry_ack_id = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(retry_ack_id, PendingCompletion::Succeed)
            .expect("delayed ACK retry succeeds");
        let _ = take_frames(&mut cluster, child_a);
        assert_eq!(cluster.resource_counts().unacked_messages, 0);

        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::Close,
                ScriptedBehavior::Delay,
            )
            .expect("close delay scripted");
        send(&mut cluster, child_a, &close_command(11, 6)).expect("close held");
        assert_eq!(cluster.resource_counts().child_consumers, 1);
        let close_id = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(
                close_id,
                PendingCompletion::Fail(BrokerFailure::new(
                    pb::ServerError::ServiceNotReady,
                    "close failed",
                )),
            )
            .expect("delayed close fails");
        assert!(
            take_frames(&mut cluster, child_a)[0]
                .command
                .error
                .is_some()
        );
        assert_eq!(cluster.resource_counts().child_consumers, 1);
        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::Close,
                ScriptedBehavior::Delay,
            )
            .expect("close retry delay scripted");
        send(&mut cluster, child_a, &close_command(11, 7)).expect("close retry held");
        let retry_close_id = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(retry_close_id, PendingCompletion::Succeed)
            .expect("delayed close retry succeeds");
        let _ = take_frames(&mut cluster, child_a);
        assert_eq!(cluster.resource_counts().child_consumers, 0);
        assert_eq!(cluster.resource_counts().pending_operations, 0);
    }

    #[test]
    fn immediate_open_ack_and_close_failures_preserve_authoritative_state() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let topic = cluster.topic().to_owned();
        cluster
            .script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    pb::ServerError::ServiceNotReady,
                    "controller open failed",
                )),
            )
            .expect("controller failure scripted");
        send(
            &mut cluster,
            controller,
            &scalable_subscribe_command(&topic, "sub", "failed-member", 1, 1),
        )
        .expect("controller emits a failure response");
        assert!(
            take_frames(&mut cluster, controller)[0]
                .command
                .scalable_topic_subscribe_response
                .as_ref()
                .expect("SubscribeResponse")
                .error
                .is_some()
        );
        assert_eq!(cluster.resource_counts().scalable_members, 0);
        assert!(cluster.ownership_history.is_empty());
        assert!(cluster.assignment_contexts.is_empty());

        cluster
            .script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Delay,
            )
            .expect("delayed controller failure scripted");
        send(
            &mut cluster,
            controller,
            &scalable_subscribe_command(&topic, "sub", "failed-delayed", 3, 3),
        )
        .expect("controller holds the delayed registration");
        let delayed = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(
                delayed,
                PendingCompletion::Fail(BrokerFailure::new(
                    pb::ServerError::ServiceNotReady,
                    "delayed controller open failed",
                )),
            )
            .expect("delayed registration fails");
        let _ = take_frames(&mut cluster, controller);
        assert!(cluster.ownership_history.is_empty());
        assert!(cluster.assignment_contexts.is_empty());

        register_member(&mut cluster, controller, "child-sub", "child-member", 2, 20);

        let child = connect(&mut cluster, Endpoint::Segment(1));
        let segment_topic = cluster.segment_topic(1).expect("segment topic");
        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::SegmentOpen,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    pb::ServerError::ServiceNotReady,
                    "child open failed",
                )),
            )
            .expect("child failure scripted");
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &segment_topic,
                "child-sub",
                "child-member",
                1,
                10,
                2,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("child emits a failure response");
        assert!(take_frames(&mut cluster, child)[0].command.error.is_some());
        assert_eq!(cluster.resource_counts().child_consumers, 0);

        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &segment_topic,
                "child-sub",
                "child-member",
                1,
                11,
                3,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("open succeeds after one-shot failure");
        let _ = take_frames(&mut cluster, child);
        cluster
            .enqueue_message(1, Bytes::from_static(b"retryable"))
            .expect("message enqueued");
        send(&mut cluster, child, &flow_command(11, 1)).expect("FLOW accepted");
        let delivered = take_frames(&mut cluster, child);
        let delivered_id = message_frame(&delivered)
            .command
            .message
            .as_ref()
            .expect("CommandMessage")
            .message_id
            .clone();

        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    pb::ServerError::PersistenceError,
                    "ack failed",
                )),
            )
            .expect("ACK failure scripted");
        send(
            &mut cluster,
            child,
            &ack_command(11, 4, delivered_id.clone()),
        )
        .expect("ACK emits a failure response");
        assert!(
            take_frames(&mut cluster, child)[0]
                .command
                .ack_response
                .as_ref()
                .expect("AckResponse")
                .error
                .is_some()
        );
        assert_eq!(cluster.resource_counts().unacked_messages, 1);

        cluster
            .script_next(
                Endpoint::Segment(1),
                OperationKind::Close,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    pb::ServerError::ServiceNotReady,
                    "close failed",
                )),
            )
            .expect("close failure scripted");
        send(&mut cluster, child, &close_command(11, 5)).expect("close emits a failure response");
        assert!(take_frames(&mut cluster, child)[0].command.error.is_some());
        assert_eq!(cluster.resource_counts().child_consumers, 1);

        send(&mut cluster, child, &ack_command(11, 6, delivered_id))
            .expect("ACK succeeds after one-shot failure");
        let _ = take_frames(&mut cluster, child);
        send(&mut cluster, child, &close_command(11, 7))
            .expect("close succeeds after one-shot failure");
        let _ = take_frames(&mut cluster, child);
        assert_eq!(cluster.resource_counts().child_consumers, 0);
        assert_eq!(cluster.resource_counts().unacked_messages, 0);
    }

    #[test]
    fn assignment_frontier_adds_active_children_only_after_parent_drain() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let member = register_member(&mut cluster, controller, "sub", "member", 1, 1);
        cluster
            .advance_layout(2, split_layout(2))
            .expect("segment 1 splits");

        assert!(matches!(
            cluster.publish_assignment_plan(2, vec![FullAssignment::new(member, [1, 2, 3, 4])]),
            Err(M1FakeError::InvalidAssignment(_))
        ));
        cluster
            .publish_assignment_plan(2, vec![FullAssignment::new(member, [1, 2])])
            .expect("sealed segment and active root form the pre-drain frontier");
        let before = take_frames(&mut cluster, controller);
        let before_ids: Vec<_> = before[0]
            .command
            .scalable_topic_assignment_update
            .as_ref()
            .expect("assignment update")
            .assignment
            .segments
            .iter()
            .map(|segment| segment.segment_id)
            .collect();
        assert_eq!(before_ids, vec![1, 2]);

        let _ = complete_empty_segment(&mut cluster, "sub", "member", 1, 7, 2);
        cluster
            .publish_assignment_plan(2, vec![FullAssignment::new(member, [1, 2, 3, 4])])
            .expect("drained parent admits both active children");
        let after = take_frames(&mut cluster, controller);
        let after_ids: Vec<_> = after[0]
            .command
            .scalable_topic_assignment_update
            .as_ref()
            .expect("assignment update")
            .assignment
            .segments
            .iter()
            .map(|segment| segment.segment_id)
            .collect();
        assert_eq!(after_ids, vec![1, 2, 3, 4]);
    }

    #[test]
    fn historical_assignments_use_retained_epoch_context_after_completion_and_reconnect() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let member = register_member(&mut cluster, controller, "sub", "member", 1, 1);
        let group = cluster
            .memberships
            .get(&member)
            .expect("registered member")
            .group
            .clone();
        cluster
            .advance_layout(2, split_layout(2))
            .expect("segment 1 splits");
        cluster
            .publish_assignment_plan(2, vec![FullAssignment::new(member, [1, 2])])
            .expect("pre-drain epoch context is authoritative");
        let _ = take_frames(&mut cluster, controller);

        cluster.completed_segments.insert((group.clone(), 1));
        cluster
            .advance_layout(3, split_layout(2))
            .expect("same topology advances after parent completion");
        cluster.remove_membership(member, true);
        let replacement = MemberId::new(controller, 9);
        cluster.create_membership(replacement, group.clone(), "member".to_owned());
        cluster
            .register_membership(replacement)
            .expect("replacement keeps the stable consumer identity");

        assert_eq!(
            cluster.assignment_segment_ids(2, &group),
            BTreeSet::from([1, 2]),
            "later completion must not add children to an old assignment epoch"
        );
        cluster
            .publish_assignment_plan(2, vec![FullAssignment::new(replacement, [1, 2])])
            .expect("a reconnect can receive its retained historical snapshot");
    }

    #[test]
    fn historical_assignment_plan_emits_without_mutating_current_ownership() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let member_a = register_member(&mut cluster, controller, "sub", "member-a", 1, 1);
        let member_b = register_member(&mut cluster, controller, "sub", "member-b", 2, 2);
        cluster
            .advance_layout(2, split_layout(2))
            .expect("segment 1 splits");
        cluster
            .publish_assignment_plan(
                2,
                vec![
                    FullAssignment::new(member_a, [1]),
                    FullAssignment::new(member_b, [2]),
                ],
            )
            .expect("current frontier installed");
        let _ = take_frames(&mut cluster, controller);

        cluster
            .publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(member_a, [2]),
                    FullAssignment::new(member_b, [1]),
                ],
            )
            .expect("historical swapped plan is emitted only");
        cluster
            .resend_assignment(member_a)
            .expect("member A current assignment remains available");
        cluster
            .resend_assignment(member_b)
            .expect("member B current assignment remains available");
        let frames = take_frames(&mut cluster, controller);
        let snapshots: Vec<_> = frames
            .iter()
            .filter_map(|frame| frame.command.scalable_topic_assignment_update.as_ref())
            .map(|update| {
                (
                    update.consumer_id,
                    update.assignment.layout_epoch,
                    update
                        .assignment
                        .segments
                        .iter()
                        .map(|segment| segment.segment_id)
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(
            snapshots,
            vec![
                (1, 1, vec![2]),
                (2, 1, vec![1]),
                (1, 2, vec![1]),
                (2, 2, vec![2]),
            ]
        );

        let child = connect(&mut cluster, Endpoint::Segment(1));
        let topic = cluster.segment_topic(1).expect("segment topic");
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "sub",
                "member-a",
                1,
                7,
                3,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("current member A ownership was not swapped by historical plan");
        assert!(
            take_frames(&mut cluster, child)[0]
                .command
                .success
                .is_some()
        );
    }

    #[test]
    fn early_descendant_assignment_keeps_independent_strict_barrier_evidence() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let member = register_member(&mut cluster, controller, "sub", "member-a", 1, 100);
        assert_eq!(cluster.member("sub", "member-a"), Some(member));
        cluster
            .advance_layout(2, split_layout(2))
            .expect("split layout accepted");
        let _ = take_frames(&mut cluster, controller);

        cluster
            .publish_early_descendant_assignment_plan(
                2,
                vec![FullAssignment::new(member, [1, 2, 3, 4])],
            )
            .expect("complete early-descendant assignment accepted");
        let update = take_frames(&mut cluster, controller)
            .pop()
            .expect("assignment update")
            .command
            .scalable_topic_assignment_update
            .expect("ScalableTopicAssignmentUpdate");
        assert_eq!(
            update
                .assignment
                .segments
                .iter()
                .map(|segment| segment.segment_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            cluster
                .drain_eligibility(member, 3)
                .expect("fake independently classifies ancestry"),
            DrainEligibility::ParentBlocked {
                segment_ids: vec![1]
            }
        );
    }

    #[test]
    fn assignments_cover_equal_epoch_duplicate_stale_and_push_before_response() {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let topic = cluster.topic().to_owned();
        send(
            &mut cluster,
            controller,
            &scalable_lookup_command(90, &topic),
        )
        .expect("layout lookup accepted");
        let lookup_frames = take_frames(&mut cluster, controller);
        assert_eq!(
            lookup_frames.len(),
            2,
            "response plus M1 duplicate baseline"
        );
        let epochs: Vec<_> = lookup_frames
            .iter()
            .map(|frame| {
                frame
                    .command
                    .scalable_topic_update
                    .as_ref()
                    .expect("ScalableTopicUpdate")
                    .dag
                    .as_ref()
                    .expect("full DAG")
                    .epoch
            })
            .collect();
        assert_eq!(epochs, vec![1, 1]);

        send(
            &mut cluster,
            controller,
            &scalable_subscribe_command(&topic, "sub", "member-a", 1, 100),
        )
        .expect("first member opens");
        let initial = take_frames(&mut cluster, controller)
            .pop()
            .expect("initial assignment")
            .command
            .scalable_topic_subscribe_response
            .expect("SubscribeResponse")
            .assignment
            .expect("full assignment");
        assert_eq!(
            initial
                .segments
                .iter()
                .map(|segment| segment.segment_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(initial.segments[0].hash_end, 32_767);
        assert_eq!(initial.segments[1].hash_start, 32_768);
        assert_eq!(
            initial
                .segments
                .iter()
                .map(|segment| segment.segment_topic.as_str())
                .collect::<Vec<_>>(),
            vec![
                "segment://public/default/scaled/0000-7fff-1",
                "segment://public/default/scaled/8000-ffff-2",
            ]
        );

        cluster
            .script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Delay,
            )
            .expect("controller delay scripted");
        send(
            &mut cluster,
            controller,
            &scalable_subscribe_command(&topic, "sub", "member-b", 2, 101),
        )
        .expect("second member held");
        let member_a = MemberId::new(controller, 1);
        let member_b = MemberId::new(controller, 2);
        assert_eq!(cluster.member("sub", "member-a"), Some(member_a));
        assert_eq!(
            cluster.member("sub", "member-b"),
            Some(member_b),
            "semantic lookup includes a delayed scalable open"
        );
        cluster
            .publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(member_a, [1]),
                    FullAssignment::new(member_b, [2]),
                ],
            )
            .expect("changed equal-epoch rebalance accepted");
        let pending = cluster.pending_operations()[0].id;
        cluster
            .complete_pending(pending, PendingCompletion::Succeed)
            .expect("second response released");
        let ordered = take_frames(&mut cluster, controller);
        assert_eq!(ordered.len(), 3, "A update, B push, then B response");
        assert_eq!(
            ordered[1].command.r#type,
            pb::base_command::Type::ScalableTopicAssignmentUpdate as i32
        );
        assert_eq!(
            ordered[1]
                .command
                .scalable_topic_assignment_update
                .as_ref()
                .expect("B assignment push")
                .consumer_id,
            2
        );
        assert_eq!(
            ordered[2].command.r#type,
            pb::base_command::Type::ScalableTopicSubscribeResponse as i32,
            "the assignment push precedes the delayed response"
        );

        let invalid = cluster.publish_assignment_plan(
            1,
            vec![
                FullAssignment::new(member_a, [1]),
                FullAssignment::new(member_b, []),
            ],
        );
        assert!(matches!(invalid, Err(M1FakeError::InvalidAssignment(_))));

        cluster
            .advance_layout(2, split_layout(2))
            .expect("split layout accepted");
        let active_only = cluster.publish_assignment_plan(
            2,
            vec![
                FullAssignment::new(member_a, [3, 4]),
                FullAssignment::new(member_b, [2]),
            ],
        );
        assert!(matches!(
            active_only,
            Err(M1FakeError::InvalidAssignment(_))
        ));
        let _ = take_frames(&mut cluster, controller);
        let _ = complete_empty_segment(&mut cluster, "sub", "member-a", 1, 50, 150);
        cluster
            .publish_assignment_plan(
                2,
                vec![
                    FullAssignment::new(member_a, [1, 3, 4]),
                    FullAssignment::new(member_b, [2]),
                ],
            )
            .expect("epoch-2 assignment accepted");
        let epoch_two_updates = take_frames(&mut cluster, controller);
        let member_a_assignment = epoch_two_updates
            .iter()
            .filter_map(|frame| frame.command.scalable_topic_assignment_update.as_ref())
            .find(|update| update.consumer_id == 1)
            .expect("member A receives its full assignment");
        assert_eq!(
            member_a_assignment
                .assignment
                .segments
                .iter()
                .map(|segment| segment.segment_id)
                .collect::<Vec<_>>(),
            vec![1, 3, 4],
            "sealed ancestors remain part of the complete assignment"
        );

        cluster
            .resend_assignment(member_b)
            .expect("duplicate assignment queued");
        assert!(matches!(
            cluster.push_stale_assignment(member_b, 1, [3]),
            Err(M1FakeError::InvalidAssignment(_))
        ));
        cluster
            .push_stale_assignment(member_b, 1, [2])
            .expect("stale assignment queued without mutation");
        cluster
            .resend_assignment(member_b)
            .expect("authoritative assignment remains current");
        let assignment_epochs: Vec<_> = take_frames(&mut cluster, controller)
            .into_iter()
            .map(|frame| {
                frame
                    .command
                    .scalable_topic_assignment_update
                    .expect("AssignmentUpdate")
                    .assignment
                    .layout_epoch
            })
            .collect();
        assert_eq!(assignment_epochs, vec![2, 1, 2]);

        cluster
            .resend_layout(controller, 90)
            .expect("duplicate layout queued");
        cluster
            .push_stale_layout(controller, 90, 1)
            .expect("stale layout queued without rollback");
        let layout_epochs: Vec<_> = take_frames(&mut cluster, controller)
            .into_iter()
            .map(|frame| {
                frame
                    .command
                    .scalable_topic_update
                    .expect("ScalableTopicUpdate")
                    .dag
                    .expect("DAG")
                    .epoch
            })
            .collect();
        assert_eq!(layout_epochs, vec![2, 1]);

        let close = pb::BaseCommand {
            r#type: pb::base_command::Type::ScalableTopicClose as i32,
            scalable_topic_close: Some(pb::CommandScalableTopicClose { session_id: 90 }),
            ..Default::default()
        };
        send(&mut cluster, controller, &close).expect("layout watch closes");
        let counts = cluster.resource_counts();
        assert_eq!(counts.layout_sessions, 0);
        assert_eq!(
            counts.scalable_members, 2,
            "M1 layout close has no pooled member-unregister command"
        );
    }

    #[test]
    fn multi_member_reconnect_preserves_each_disconnected_baseline_share() {
        let mut cluster = M1FakeCluster::two_segment();
        let topic = cluster.topic().to_owned();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let member_a = register_member(&mut cluster, controller, "sub", "member-a", 1, 1);
        let member_b = register_member(&mut cluster, controller, "sub", "member-b", 2, 2);
        cluster
            .publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(member_a, [1]),
                    FullAssignment::new(member_b, [2]),
                ],
            )
            .expect("two-member baseline installed");
        let _ = take_frames(&mut cluster, controller);
        cluster
            .disconnect_connection(controller)
            .expect("controller incarnation disconnects");

        let early_child = connect(&mut cluster, Endpoint::Segment(2));
        let segment_two = cluster.segment_topic(2).expect("segment two topic");
        send(
            &mut cluster,
            early_child,
            &segment_subscribe_command(
                &segment_two,
                "sub",
                "member-b",
                2,
                5,
                5,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("child racing replacement controller receives a broker response");
        let busy = take_frames(&mut cluster, early_child)[0]
            .command
            .error
            .as_ref()
            .expect("generated ConsumerBusy during reconnect gap")
            .error;
        assert_eq!(busy, pb::ServerError::ConsumerBusy as i32);

        let replacement = connect(&mut cluster, Endpoint::Controller);
        send(
            &mut cluster,
            replacement,
            &scalable_subscribe_command(&topic, "sub", "member-a", 1, 3),
        )
        .expect("member A reconnects first");
        let assignment_a = take_frames(&mut cluster, replacement)[0]
            .command
            .scalable_topic_subscribe_response
            .as_ref()
            .and_then(|response| response.assignment.as_ref())
            .expect("member A assignment")
            .segments
            .iter()
            .map(|segment| segment.segment_id)
            .collect::<Vec<_>>();
        assert_eq!(assignment_a, vec![1]);

        send(
            &mut cluster,
            replacement,
            &scalable_subscribe_command(&topic, "sub", "member-b", 2, 4),
        )
        .expect("member B reconnects second");
        let assignment_b = take_frames(&mut cluster, replacement)[0]
            .command
            .scalable_topic_subscribe_response
            .as_ref()
            .and_then(|response| response.assignment.as_ref())
            .expect("member B assignment")
            .segments
            .iter()
            .map(|segment| segment.segment_id)
            .collect::<Vec<_>>();
        assert_eq!(assignment_b, vec![2]);
    }

    #[test]
    fn physical_disconnect_releases_owners_and_reconnect_uses_a_fresh_baseline() {
        let mut cluster = M1FakeCluster::two_segment();
        let topic = cluster.topic().to_owned();
        let controller = connect(&mut cluster, Endpoint::Controller);
        send(
            &mut cluster,
            controller,
            &scalable_subscribe_command(&topic, "sub", "stable-member", 8, 1),
        )
        .expect("member opens");
        let before = take_frames(&mut cluster, controller)
            .pop()
            .expect("baseline response")
            .command
            .scalable_topic_subscribe_response
            .expect("SubscribeResponse")
            .assignment
            .expect("assignment");
        assert_eq!(before.segments.len(), 2);
        cluster
            .disconnect_connection(controller)
            .expect("controller disconnects");
        assert_eq!(cluster.resource_counts().scalable_members, 0);

        let reconnected = connect(&mut cluster, Endpoint::Controller);
        send(
            &mut cluster,
            reconnected,
            &scalable_subscribe_command(&topic, "sub", "stable-member", 8, 2),
        )
        .expect("same logical member reconnects");
        let after = take_frames(&mut cluster, reconnected)
            .pop()
            .expect("reconnect response")
            .command
            .scalable_topic_subscribe_response
            .expect("SubscribeResponse")
            .assignment
            .expect("assignment");
        assert_eq!(after, before);
        assert_ne!(
            MemberId::new(controller, 8),
            MemberId::new(reconnected, 8),
            "the fake keeps connection incarnations distinct"
        );

        let child = connect(&mut cluster, Endpoint::Segment(1));
        let segment_topic = cluster.segment_topic(1).expect("segment topic");
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &segment_topic,
                "sub",
                "stable-member",
                1,
                9,
                3,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("child opens");
        let _ = take_frames(&mut cluster, child);
        cluster
            .disconnect_connection(child)
            .expect("child connection disconnects");
        assert_eq!(cluster.resource_counts().child_consumers, 0);
        let child_reconnected = connect(&mut cluster, Endpoint::Segment(1));
        send(
            &mut cluster,
            child_reconnected,
            &segment_subscribe_command(
                &segment_topic,
                "sub",
                "stable-member",
                1,
                9,
                4,
                pb::command_subscribe::SubType::Exclusive,
            ),
        )
        .expect("physical child loss releases Exclusive ownership");
        assert!(
            take_frames(&mut cluster, child_reconnected)[0]
                .command
                .success
                .is_some()
        );
        assert_eq!(cluster.resource_counts().child_consumers, 1);

        assert_eq!(
            cluster
                .disconnect_endpoint(Endpoint::Controller)
                .expect("controller endpoint exists"),
            1
        );
        assert_eq!(
            cluster
                .disconnect_endpoint(Endpoint::Segment(1))
                .expect("child endpoint exists"),
            1
        );
        assert_eq!(cluster.resource_counts().connections, 0);
        assert_eq!(cluster.resource_counts().scalable_members, 0);
        assert_eq!(cluster.resource_counts().child_consumers, 0);
    }

    fn ancestry_cluster() -> (M1FakeCluster, ConnectionId, MemberId, MemberId) {
        let mut cluster = M1FakeCluster::two_segment();
        let controller = connect(&mut cluster, Endpoint::Controller);
        let topic = cluster.topic().to_owned();
        send(
            &mut cluster,
            controller,
            &scalable_subscribe_command(&topic, "sub", "member-a", 1, 1),
        )
        .expect("member A opens");
        send(
            &mut cluster,
            controller,
            &scalable_subscribe_command(&topic, "sub", "member-b", 2, 2),
        )
        .expect("member B opens");
        let _ = take_frames(&mut cluster, controller);
        let member_a = MemberId::new(controller, 1);
        let member_b = MemberId::new(controller, 2);
        cluster
            .publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(member_a, [1]),
                    FullAssignment::new(member_b, [2]),
                ],
            )
            .expect("initial ownership plan");
        cluster
            .advance_layout(2, split_layout(2))
            .expect("segment 1 splits");
        let _ = take_frames(&mut cluster, controller);
        let _ = complete_empty_segment(&mut cluster, "sub", "member-a", 1, 50, 50);
        (cluster, controller, member_a, member_b)
    }

    #[test]
    fn ancestry_scenarios_distinguish_local_from_cross_member_evidence() {
        let (mut local, _, member_a, member_b) = ancestry_cluster();
        local
            .publish_assignment_plan(
                2,
                vec![
                    FullAssignment::new(member_a, [1, 3, 4]),
                    FullAssignment::new(member_b, [2]),
                ],
            )
            .expect("children stay with their parent owner");
        assert_eq!(
            local
                .ancestry_proof(member_a, 3)
                .expect("local proof available"),
            AncestryProof::LocallyProvable {
                member: member_a,
                parent_ids: vec![1],
            }
        );

        let (mut cross, _, member_a, member_b) = ancestry_cluster();
        cross
            .publish_assignment_plan(
                2,
                vec![
                    FullAssignment::new(member_a, [1, 4]),
                    FullAssignment::new(member_b, [2, 3]),
                ],
            )
            .expect("one child moves to another member");
        assert_eq!(
            cross
                .ancestry_proof(member_b, 3)
                .expect("cross-member evidence available"),
            AncestryProof::CrossMemberUnprovable {
                child_member: member_b,
                parent_members: vec![member_a],
                parent_ids: vec![1],
            }
        );
        assert_eq!(
            cross
                .drain_eligibility(member_b, 3)
                .expect("cross-member drain classification available"),
            DrainEligibility::CrossMemberUnprovable {
                segment_ids: vec![1]
            }
        );
    }
}
