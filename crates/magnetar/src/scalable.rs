// SPDX-License-Identifier: Apache-2.0

//! **Experimental** (PIP-460, ADR-0031) — scalable-topic `StreamConsumer`.
//!
//! PIP-460 introduces a `topic://<...>` URL scheme backed by a controller
//! broker and a segment DAG. magnetar v0.2.0 ships **only** the
//! [`StreamConsumer`] happy path, behind the default-off `scalable-topics`
//! feature, with **drop-on-DAG-change** semantics (no transparent segment
//! failover). See [ADR-0031](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0031-pip-460-scalable-subscription-scope.md)
//! and the [proposal](https://github.com/CleverCloud/magnetar/blob/main/specs/proposals/pip-460-scalable-topics.md).
//!
//! # Surface
//!
//! [`ScalableTopicsApi`] is the engine-side hook (re-exported from the engine
//! module): each runtime implements it on its `Client` type. [`StreamConsumer`]
//! is generic over `E: Engine where E::ClientState: ScalableTopicsApi` per
//! ADR-0026 §D1 — the same extension-trait pattern the transaction / producer /
//! consumer surfaces use, so it composes with the engine-generic
//! [`crate::PulsarClient<E>`] without GAT growth.
//!
//! # Drop-on-change (v0.2.0)
//!
//! When the controller broker pushes a segment split / merge / removal while
//! the [`StreamConsumer`] is active, the runtime surfaces
//! [`ConsumerEvent::DagChanged`]; the caller must re-resolve and re-subscribe.
//! Transparent failover, in-place repartition, `QueueConsumer`,
//! `CheckpointConsumer`, and controller-election awareness are explicit
//! v0.3.0+ work (out of scope, ADR-0031).

// The PIP-460 surface doc-comments thread bare type names (`StreamConsumer`,
// `DagWatch`, …) through prose where backticking every occurrence hurts
// readability — same stance the proto crate takes for the protocol docs.
#![allow(clippy::doc_markdown)]

use std::marker::PhantomData;

/// **Experimental** (PIP-460 v0.2.0). Why the segment DAG changed under a live
/// [`StreamConsumer`]. Re-exported from the proto layer so callers match on a
/// single canonical type.
pub use magnetar_proto::DagChangeReason;
/// **Experimental** (PIP-460 v0.2.0). One node of a scalable topic's segment
/// DAG. Re-exported from the proto layer.
pub use magnetar_proto::{KeyRange, SegmentDescriptor, SegmentId, SegmentState};

use crate::Engine;
pub use crate::engine::{ScalableEvent, ScalableLookup, ScalableTopicsApi};

/// **Experimental** (PIP-460 v0.2.0). An event surfaced by [`StreamConsumer::next_event`].
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
        /// Watch session id the update belongs to.
        watch_session_id: u64,
    },
    /// The segment DAG changed while consuming (split / merge / removal). The
    /// `StreamConsumer` has closed its per-segment consumers; re-resolve and
    /// re-subscribe to continue. This is the v0.2.0 "drop-on-change"
    /// guarantee.
    DagChanged {
        /// Watch session id whose DAG changed.
        watch_session_id: u64,
        /// Why the DAG changed.
        reason: DagChangeReason,
    },
    /// The DAG-watch session closed (controller-broker disconnect or client
    /// close). No automatic re-lookup in v0.2.0.
    Closed {
        /// Watch session id that closed.
        watch_session_id: u64,
        /// Optional close reason.
        reason: Option<String>,
    },
}

/// **Experimental** (PIP-460 v0.2.0, ADR-0031). StreamConsumer over a scalable
/// topic. Holds an open DAG-watch session against the controller broker and
/// surfaces [`ConsumerEvent`]s. **Drops on DAG change** — no transparent
/// segment failover in v0.2.0.
///
/// `T` is the (future) per-message payload type; in the v0.2.0 scaffold the
/// surface is DAG-watch-centric (the per-segment v4 consumer fan-out and typed
/// receive land once a Pulsar 5.0 broker ships the wire surface — see ADR-0031
/// §"Out of scope"). Construct via [`crate::PulsarClient::scalable_stream_consumer`].
pub struct StreamConsumer<T, E: Engine>
where
    E::ClientState: ScalableTopicsApi,
{
    client: crate::PulsarClient<E>,
    topic: String,
    watch_session_id: u64,
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
            .field("watch_session_id", &self.watch_session_id)
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

    /// The watch session id backing this StreamConsumer.
    #[must_use]
    pub fn watch_session_id(&self) -> u64 {
        self.watch_session_id
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
                ScalableEvent::DagUpdated {
                    watch_session_id,
                    delta,
                } if watch_session_id == self.watch_session_id => {
                    // Apply the delta to the local snapshot.
                    for seg in &delta.added {
                        if !self.dag.iter().any(|d| d.segment_id == seg.segment_id) {
                            self.dag.push(seg.clone());
                        }
                    }
                    self.dag.retain(|d| !delta.removed.contains(&d.segment_id));
                    if delta.is_consume_affecting() {
                        // Drop-on-change: close per-segment consumers (none yet
                        // in the scaffold) and surface DagChanged.
                        self.dropped = true;
                        return Some(ConsumerEvent::DagChanged {
                            watch_session_id,
                            reason: delta.change_reason(),
                        });
                    }
                    return Some(ConsumerEvent::DagUpdated { watch_session_id });
                }
                ScalableEvent::DagChangedDuringConsume {
                    watch_session_id,
                    reason,
                } if watch_session_id == self.watch_session_id => {
                    self.dropped = true;
                    return Some(ConsumerEvent::DagChanged {
                        watch_session_id,
                        reason,
                    });
                }
                ScalableEvent::DagWatchClosed {
                    watch_session_id,
                    reason,
                } if watch_session_id == self.watch_session_id => {
                    return Some(ConsumerEvent::Closed {
                        watch_session_id,
                        reason,
                    });
                }
                // Events for other sessions / stray lookup-resolveds — skip
                // and keep waiting for the next one.
                _ => {}
            }
        }
    }

    /// Close the DAG-watch session and tear down the StreamConsumer.
    pub fn close(self) {
        self.client.inner.close_dag_watch(self.watch_session_id);
    }
}

impl<E: Engine> crate::PulsarClient<E>
where
    E::ClientState: ScalableTopicsApi,
{
    /// **Experimental** (PIP-460 v0.2.0, ADR-0031). Open a scalable-topic
    /// [`StreamConsumer`] for a `topic://...` URL. Resolves the topic against
    /// the controller broker, opens a DAG-watch session seeded with the
    /// current segment DAG, and returns a consumer that surfaces
    /// [`ConsumerEvent`]s (drop-on-change).
    ///
    /// # Errors
    ///
    /// Returns the runtime client error if the scalable lookup fails (e.g. the
    /// broker does not support PIP-460, or the topic is not a scalable topic).
    pub async fn scalable_stream_consumer<T>(
        &self,
        topic: impl Into<String>,
    ) -> Result<StreamConsumer<T, E>, <E::ClientState as ScalableTopicsApi>::Error>
    where
        E::ClientState: Clone,
    {
        let topic = topic.into();
        let lookup = self.inner.scalable_topic_lookup(&topic).await?;
        let watch_session_id =
            self.inner
                .open_dag_watch(&topic, lookup.lookup_token, lookup.segments.clone());
        Ok(StreamConsumer {
            client: crate::PulsarClient {
                inner: self.inner.clone(),
                memory_limit: self.memory_limit,
            },
            topic,
            watch_session_id,
            dag: lookup.segments,
            dropped: false,
            _payload: PhantomData,
        })
    }

    /// **Experimental** (PIP-460 v0.2.0, ADR-0031). Resolve a `topic://...`
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
