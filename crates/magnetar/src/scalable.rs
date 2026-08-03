// SPDX-License-Identifier: Apache-2.0

//! **Experimental** (PIP-460, ADR-0093) — scalable-topic `StreamConsumer`.
//!
//! PIP-460 introduces a `topic://<...>` URL scheme backed by a controller
//! broker and a segment DAG. magnetar currently ships **only** the
//! `StreamConsumer` happy path, behind the default-off `scalable-topics`
//! feature, with **drop-on-DAG-change** semantics (no transparent segment
//! failover). See [ADR-0031](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0031-pip-460-scalable-subscription-scope.md)
//! and the [proposal](https://github.com/CleverCloud/magnetar/blob/main/specs/proposals/pip-460-scalable-topics.md).
//!
//! # Surface
//!
//! [`ScalableTopicsApi`] is the engine-side hook (re-exported from the engine
//! module): each runtime implements it on its `Client` type. `StreamConsumer`
//! is generic over `E: Engine where E::ClientState: ScalableTopicsApi` per
//! ADR-0026 §D1 — the same extension-trait pattern the transaction / producer /
//! consumer surfaces use, so it composes with the engine-generic
//! [`crate::PulsarClient<E>`] without GAT growth.
//!
//! # Drop-on-change
//!
//! When the controller broker pushes a segment split / merge / removal while
//! the `StreamConsumer` is active, the runtime surfaces
//! `ConsumerEvent::DagChanged`; the caller must re-resolve and re-subscribe.
//! Transparent failover, in-place repartition, `QueueConsumer`,
//! `CheckpointConsumer`, and controller-election awareness are explicit
//! future work (out of scope, ADR-0031).

// The PIP-460 surface doc-comments thread bare type names (`StreamConsumer`,
// `DagWatch`, …) through prose where backticking every occurrence hurts
// readability — same stance the proto crate takes for the protocol docs.
#![allow(clippy::doc_markdown)]

use std::marker::PhantomData;

/// **Experimental** (PIP-460). Why the segment DAG changed under a live
/// [`StreamConsumer`]. Re-exported from the proto layer so callers match on a
/// single canonical type.
pub use magnetar_proto::DagChangeReason;
/// **Experimental** (PIP-460). The controller leader's answer to a consumer
/// registration: which `segment://` topics this consumer owns. Re-exported from
/// the proto layer.
pub use magnetar_proto::{
    AssignedSegment, AssignmentDelta, ConsumerAssignment, ScalableConsumerType, TcAssignment,
    TopicsChange,
};
/// **Experimental** (PIP-460). One node of a scalable topic's segment
/// DAG. Re-exported from the proto layer.
pub use magnetar_proto::{KeyRange, SegmentDescriptor, SegmentId, SegmentState};

use crate::Engine;
pub use crate::engine::{ScalableEvent, ScalableLookup, ScalableTopicsApi};

/// **Experimental** (PIP-460). An event surfaced by [`StreamConsumer::next_event`].
///
/// `StreamConsumer` drops its per-segment consumers on a DAG change; the
/// [`Self::DagChanged`] variant is the caller's signal to re-resolve and
/// re-subscribe.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ConsumerEvent {
    /// The segment DAG was updated (segments added / removed, or a split /
    /// merge landed). The current snapshot is available via
    /// [`StreamConsumer::dag`].
    DagUpdated {
        /// Session id the update belongs to.
        session_id: u64,
        /// Layout epoch the update moved the session to.
        epoch: u64,
    },
    /// The segment DAG changed while consuming (split / merge / removal). The
    /// `StreamConsumer` has closed its per-segment consumers; re-resolve and
    /// re-subscribe to continue. This is the "drop-on-change"
    /// guarantee.
    DagChanged {
        /// Session id whose DAG changed.
        session_id: u64,
        /// Why the DAG changed.
        reason: DagChangeReason,
    },
    /// The scalable-topic session closed (broker rejection or client close).
    /// No automatic re-lookup.
    Closed {
        /// Session id that closed.
        session_id: u64,
        /// Optional close reason.
        reason: Option<String>,
    },
}

/// **Experimental** (PIP-460, ADR-0093). StreamConsumer over a scalable
/// topic. Holds an open DAG-watch session against the controller broker and
/// surfaces [`ConsumerEvent`]s. **Drops on DAG change** — no transparent
/// segment failover.
///
/// `T` is the (future) per-message payload type; in the current scaffold the
/// surface is DAG-watch-centric (the per-segment v4 consumer fan-out and typed
/// receive land once a Pulsar 5.0 broker ships the wire surface — see ADR-0031
/// §"Out of scope"). Construct via [`crate::PulsarClient::scalable_stream_consumer`].
pub struct StreamConsumer<T, E: Engine>
where
    E::ClientState: ScalableTopicsApi,
{
    client: crate::PulsarClient<E>,
    topic: String,
    session_id: u64,
    /// Canonical `topic://...` identity the broker resolved to.
    resolved_topic_name: Option<String>,
    /// Layout epoch of the current snapshot.
    epoch: u64,
    /// Current segment DAG snapshot, kept in sync with the watch session.
    dag: Vec<SegmentDescriptor>,
    /// `true` once a DAG change dropped the per-segment consumers.
    dropped: bool,
    _payload: PhantomData<fn() -> T>,
}

// Manual `Debug` so the impl doesn't require `E::ClientState: Debug` — it
// renders the topic / session / DAG size, not the (possibly non-Debug) client.
impl<T, E: Engine> std::fmt::Debug for StreamConsumer<T, E>
where
    E::ClientState: ScalableTopicsApi,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamConsumer")
            .field("topic", &self.topic)
            .field("session_id", &self.session_id)
            .field("dag_segments", &self.dag.len())
            .field("dropped", &self.dropped)
            .finish_non_exhaustive()
    }
}

impl<T, E: Engine> StreamConsumer<T, E>
where
    E::ClientState: ScalableTopicsApi,
{
    /// The topic this StreamConsumer is bound to.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The scalable-topic session id backing this StreamConsumer.
    #[must_use]
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// The canonical `topic://...` identity the broker resolved to, when it
    /// supplied one. Differs from [`Self::topic`] when the caller passed a
    /// `persistent://` or short-form name.
    #[must_use]
    pub fn resolved_topic_name(&self) -> Option<&str> {
        self.resolved_topic_name.as_deref()
    }

    /// The layout epoch of the current DAG snapshot.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The current segment DAG snapshot.
    #[must_use]
    pub fn dag(&self) -> &[SegmentDescriptor] {
        &self.dag
    }

    /// `true` once a DAG change dropped the per-segment consumers. After this
    /// flips, the caller must re-resolve + re-subscribe (drop-on-change).
    #[must_use]
    pub fn is_dropped(&self) -> bool {
        self.dropped
    }

    /// Await the next [`ConsumerEvent`]. On a DAG change this returns
    /// [`ConsumerEvent::DagChanged`] and flips [`Self::is_dropped`]; on a
    /// benign update it returns [`ConsumerEvent::DagUpdated`] and refreshes
    /// [`Self::dag`]. Returns `None` once the session closes for good.
    pub async fn next_event(&mut self) -> Option<ConsumerEvent> {
        loop {
            let ev = self.client.inner.next_scalable_event().await?;
            match ev {
                ScalableEvent::DagUpdated { session_id, delta }
                    if session_id == self.session_id =>
                {
                    // Upstream pushes whole layouts, so the local snapshot is
                    // replaced rather than patched: apply the removals, then the
                    // additions the delta reported against the previous layout.
                    self.dag.retain(|d| !delta.removed.contains(&d.segment_id));
                    for seg in &delta.added {
                        if !self.dag.iter().any(|d| d.segment_id == seg.segment_id) {
                            self.dag.push(seg.clone());
                        }
                    }
                    self.dag.sort_by_key(|d| d.segment_id);
                    self.epoch = delta.epoch;
                    if delta.is_consume_affecting() {
                        // Drop-on-change: close per-segment consumers (none yet
                        // in the scaffold) and surface DagChanged.
                        self.dropped = true;
                        return Some(ConsumerEvent::DagChanged {
                            session_id,
                            reason: delta.change_reason(),
                        });
                    }
                    return Some(ConsumerEvent::DagUpdated {
                        session_id,
                        epoch: delta.epoch,
                    });
                }
                ScalableEvent::DagChangedDuringConsume { session_id, reason }
                    if session_id == self.session_id =>
                {
                    self.dropped = true;
                    return Some(ConsumerEvent::DagChanged { session_id, reason });
                }
                ScalableEvent::DagWatchClosed { session_id, reason }
                    if session_id == self.session_id =>
                {
                    return Some(ConsumerEvent::Closed { session_id, reason });
                }
                // Events for other sessions / stray lookup-resolveds — skip
                // and keep waiting for the next one.
                _ => {}
            }
        }
    }

    /// Close the scalable-topic session and tear down the StreamConsumer.
    pub fn close(self) {
        self.client
            .inner
            .close_scalable_topic_session(self.session_id);
    }
}

impl<E: Engine> crate::PulsarClient<E>
where
    E::ClientState: ScalableTopicsApi,
{
    /// **Experimental** (PIP-460, ADR-0093). Open a scalable-topic
    /// [`StreamConsumer`] for a `topic://...` URL. Resolves the topic against
    /// the controller broker, which opens the layout session in the same
    /// round-trip, and returns a consumer that surfaces
    /// [`ConsumerEvent`]s (drop-on-change). The session opened by the lookup
    /// stays open and keeps delivering layouts — there is no second subscribe.
    ///
    /// # Errors
    ///
    /// Returns the runtime client error if the scalable lookup fails — most
    /// notably when the broker did not advertise `supports_scalable_topics`
    /// (a Pulsar 4.x peer), or when the topic is not a scalable topic.
    pub async fn scalable_stream_consumer<T>(
        &self,
        topic: impl Into<String>,
    ) -> Result<StreamConsumer<T, E>, <E::ClientState as ScalableTopicsApi>::Error>
    where
        E::ClientState: Clone,
    {
        let topic = topic.into();
        // The lookup opens the session and leaves it open — upstream has no
        // separate watch subscribe, so there is no second round-trip here.
        let lookup = self.inner.scalable_topic_lookup(&topic).await?;
        Ok(StreamConsumer {
            client: crate::PulsarClient {
                inner: self.inner.clone(),
                memory_limit: self.memory_limit,
            },
            topic,
            session_id: lookup.session_id,
            resolved_topic_name: lookup.resolved_topic_name,
            epoch: lookup.epoch,
            dag: lookup.segments,
            dropped: false,
            _payload: PhantomData,
        })
    }

    /// **Experimental** (PIP-460, ADR-0093). Register as a scalable consumer
    /// with the controller leader and await the initial assignment.
    ///
    /// This is the command that grants a **share** of a scalable topic: the
    /// returned [`ConsumerAssignment`] names the `segment://` topics this
    /// consumer owns. [`Self::scalable_stream_consumer`] resolves a topic's
    /// layout, which is a different thing — a layout says what segments exist,
    /// an assignment says which of them are yours.
    ///
    /// A `QueueConsumer` never registers (it attaches to every active and
    /// sealed segment topic directly), which is why [`ScalableConsumerType`]
    /// offers only `Stream` and `Checkpoint`.
    ///
    /// # Errors
    ///
    /// Returns the runtime client error when the broker did not advertise
    /// `supports_scalable_topics`, when it rejects the registration, or when
    /// the connection closes first.
    pub async fn scalable_topic_subscribe(
        &self,
        topic: &str,
        subscription: &str,
        consumer_name: &str,
        consumer_id: u64,
        consumer_type: ScalableConsumerType,
    ) -> Result<ConsumerAssignment, <E::ClientState as ScalableTopicsApi>::Error> {
        self.inner
            .scalable_topic_subscribe(
                topic,
                subscription,
                consumer_name,
                consumer_id,
                consumer_type,
            )
            .await
    }

    /// **Experimental** (PIP-460, ADR-0093). Await the next scalable-topic
    /// event drained by the runtime — layout updates, drop-on-change, consumer
    /// assignments, namespace-watch membership and coordinator discovery all
    /// arrive here.
    ///
    /// Resolves `None` once the connection closes. Cancel-safe: dropping the
    /// future without polling does not lose buffered events.
    pub async fn next_scalable_event(&self) -> Option<ScalableEvent> {
        self.inner.next_scalable_event().await
    }

    /// **Experimental** (PIP-460, ADR-0093). Close a scalable-topic layout
    /// session opened by [`Self::lookup_scalable_topic`].
    ///
    /// The lookup leaves the session open — upstream folds the subscribe into
    /// it — so a caller that only wanted a one-shot view must close it, or the
    /// broker keeps pushing layouts at a client that stopped listening.
    pub fn close_scalable_topic_session(&self, session_id: u64) {
        self.inner.close_scalable_topic_session(session_id);
    }

    /// **Experimental** (PIP-460, ADR-0093). Whether the connected broker
    /// advertised the PIP-460 capability.
    ///
    /// `false` against a Pulsar 4.x peer, and against a 5.x one started with
    /// `scalableTopicsEnabled=false`. Every scalable-topic call is gated on
    /// this, so it is the one check worth making before reaching for the rest
    /// of the surface.
    #[must_use]
    pub fn broker_supports_scalable_topics(&self) -> bool {
        self.inner.broker_supports_scalable_topics()
    }

    /// **Experimental** (PIP-460, ADR-0093). Open a namespace-level watch over
    /// the scalable topics matching `property_filters` (empty matches every
    /// scalable topic in the namespace).
    ///
    /// Membership changes arrive as `ScalableEvent::TopicsChanged`; the current
    /// set is available from [`Self::scalable_topics_snapshot`].
    ///
    /// # Errors
    ///
    /// Returns the runtime client error when the broker did not advertise
    /// `supports_scalable_topics`.
    pub fn watch_scalable_topics(
        &self,
        namespace: &str,
        property_filters: Vec<(String, String)>,
    ) -> Result<u64, <E::ClientState as ScalableTopicsApi>::Error> {
        self.inner
            .watch_scalable_topics(namespace, property_filters)
    }

    /// **Experimental** (PIP-460, ADR-0093). Close a namespace-level watch.
    pub fn close_scalable_topics_watch(&self, watch_id: u64) {
        self.inner.close_scalable_topics_watch(watch_id);
    }

    /// **Experimental** (PIP-460, ADR-0093). The current matching topic set for
    /// a namespace watch, or `None` for an unknown id.
    #[must_use]
    pub fn scalable_topics_snapshot(&self, watch_id: u64) -> Option<Vec<String>> {
        self.inner.scalable_topics_snapshot(watch_id)
    }

    /// **Experimental** (PIP-460 / PIP-473, ADR-0093). Whether the broker
    /// advertised metadata-driven transaction-coordinator discovery.
    ///
    /// Upstream advertises this independently of `supports_scalable_topics`, so
    /// a broker may serve scalable topics without it.
    #[must_use]
    pub fn broker_supports_tc_metadata_discovery(&self) -> bool {
        self.inner.broker_supports_tc_metadata_discovery()
    }

    /// **Experimental** (PIP-460 / PIP-473, ADR-0093). Open a
    /// transaction-coordinator discovery watch.
    ///
    /// # Errors
    ///
    /// Returns the runtime client error when the broker did not advertise
    /// `supports_tc_metadata_discovery`.
    pub fn watch_tc_assignments(
        &self,
    ) -> Result<u64, <E::ClientState as ScalableTopicsApi>::Error> {
        self.inner.watch_tc_assignments()
    }

    /// **Experimental** (PIP-460 / PIP-473, ADR-0093). Close a
    /// transaction-coordinator discovery watch.
    pub fn close_tc_assignments_watch(&self, watch_id: u64) {
        self.inner.close_tc_assignments_watch(watch_id);
    }

    /// **Experimental** (PIP-460, ADR-0093). Resolve a `topic://...`
    /// scalable topic without opening a consumer — returns the current segment
    /// DAG + controller broker. Powers the CLI `topic-info` subcommand.
    ///
    /// # Errors
    ///
    /// Returns the runtime client error if the lookup fails.
    pub async fn lookup_scalable_topic(
        &self,
        topic: &str,
    ) -> Result<ScalableLookup, <E::ClientState as ScalableTopicsApi>::Error> {
        self.inner.scalable_topic_lookup(topic).await
    }
}
