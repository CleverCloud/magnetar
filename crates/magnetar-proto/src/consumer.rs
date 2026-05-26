// SPDX-License-Identifier: Apache-2.0

//! Per-consumer state machine.
//!
//! Mirrors `org.apache.pulsar.client.impl.ConsumerImpl`. Responsibilities:
//!
//! - Bounded receiver queue (`max_receiver_queue_size`).
//! - Permit accounting → emit `CommandFlow` when the receiver queue drains below the threshold.
//! - Batch explosion: a `CommandMessage` carrying `num_messages_in_batch > 1` is split into N
//!   [`IncomingMessage`]s with `batch_index` set.
//! - Chunk reassembly: messages with `num_chunks_from_msg > 1` are buffered until all chunks
//!   arrive, then surfaced as one logical message.
//! - Dead-letter routing: when redelivery count exceeds `max_redeliver_count`, the consumer records
//!   the message id; the runtime crate is expected to publish to the DLQ topic via a sibling
//!   Producer.
//! - Seek: emits `CommandSeek`, freezes the queue until `CommandAckResponse`.
//!
//! # References
//!
//! - `ConsumerImpl.java:143` (constructor)
//! - `ConsumerImpl.java:174` (receiver queue config)
//! - `ConsumerImpl.java:528-531` (tracker construction)

use std::collections::{HashMap, VecDeque};
use std::task::Waker;

use bytes::{Buf, Bytes};
use prost::Message as _;
use slab::Slab;

use crate::error::ConsumerError;
use crate::event::IncomingMessage;
use crate::pb;
use crate::trackers::{NegativeAcksTracker, UnackedMessageTracker};
use crate::types::{ConsumerHandle, MessageId, RequestId};

/// PIP-180 / ADR-0033 shadow-topic metadata cached on a [`ConsumerState`].
///
/// Populated at subscribe time by the runtime engine via
/// [`ConsumerState::set_shadow_metadata`]. Once set, the connection's
/// receive path classifies every incoming message: if
/// [`pb::MessageMetadata::replicated_from`] is also set, the message is a
/// shadow-presented copy of an entry from `source_topic` and the connection
/// emits [`crate::event::ConnectionEvent::MessageReceivedFromShadow`]
/// instead of [`crate::event::ConnectionEvent::Message`].
///
/// `magnetar-proto` does no admin REST itself — the metadata arrives via
/// the sans-io setter described above (per ADR-0004's zero-I/O constraint).
#[derive(Debug, Clone)]
pub struct ShadowTopicMetadata {
    /// Fully-qualified source topic name (e.g. `persistent://public/default/orders`).
    /// The broker presents shadow-side messages with the source-topic ledger/entry
    /// pointers; this string lets the runtime surface the original topic to the
    /// user without re-resolving it from each message.
    pub source_topic: String,
}

/// Per-consumer state.
#[derive(Debug)]
pub struct ConsumerState {
    /// Consumer id assigned by [`Connection`](crate::Connection).
    pub handle: ConsumerHandle,
    /// Topic name.
    pub topic: String,
    /// Subscription name.
    pub subscription: String,
    /// Caller-supplied consumer name advertised on `CommandSubscribe.consumer_name`.
    /// `None` means the broker is free to assign one. Mirrors Java
    /// `Consumer#getConsumerName`.
    pub consumer_name: Option<String>,
    /// Max receiver queue size — the consumer asks the broker for permits in batches of
    /// `receiver_queue_size / 2` once half of the queue has been consumed.
    pub receiver_queue_size: usize,
    /// Number of permits the broker currently has us at (i.e., messages the broker may still
    /// push to us without our explicit consent).
    pub available_permits: u32,
    /// Number of permits we've consumed since the last flow command. Visible to the
    /// [`Connection`](crate::Connection) so it can adjust the counter when surfacing messages
    /// to the user via `pop_message` paths that bypass `ConsumerState::pop_message`.
    pub(crate) consumed_since_flow: u32,
    /// Inbound queue of messages ready to deliver to the user.
    pub queue: VecDeque<IncomingMessage>,
    /// Per-uuid chunk reassembly state.
    chunk_reassembly: HashMap<String, ChunkBuffer>,
    /// In-flight `CommandSeek` request id, if any. While `Some`, the queue is frozen.
    pub pending_seek: Option<RequestId>,
    /// Per-consumer waker slab. Each in-flight `receive()` future registers a
    /// `Waker` here via [`Self::register_receive_waker`] and evicts it on `Drop`
    /// via [`Self::cancel_receive_waker`]. When a new message arrives (or the
    /// consumer is closed / has reached end-of-topic), every parked waker is
    /// drained and woken — this lets multiple concurrent receivers fan out
    /// cleanly without one waker clobbering another.
    ///
    /// Not a channel — a `Slab<Waker>` is the canonical no-channel wake pattern
    /// (see [ADR-0003](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0003-no-channels-rule.md)).
    pub receive_wakers: Slab<Waker>,
    /// Closed flag.
    pub closed: bool,
    /// Configured max redelivery before DLQ routing kicks in (`0` disables DLQ routing).
    pub max_redeliver_count: u32,
    /// Messages flagged for DLQ routing. The runtime crate drains this and republishes.
    pub dead_letter_pending: Vec<IncomingMessage>,
    /// Mirrors Java `Consumer#pause` / `Consumer#resume`. When `true`, [`Self::maybe_flow`]
    /// stops emitting flow commands so the broker stops dispatching new messages. Already
    /// buffered messages can still be popped via [`Self::pop_message`].
    pub paused: bool,
    /// Set to `true` when the broker sends `CommandReachedEndOfTopic` for this consumer,
    /// indicating no more messages will ever be dispatched. Mirrors Java
    /// `Consumer#hasReachedEndOfTopic`.
    pub reached_end_of_topic: bool,
    /// Cumulative count of logical messages delivered to the user-facing queue. Mirrors
    /// Java `ConsumerStats#getTotalMsgsReceived`.
    pub total_msgs_received: u64,
    /// Cumulative payload bytes delivered (each message counts its `payload.len()`).
    pub total_bytes_received: u64,
    /// Cumulative count of ACK requests issued (broker may not yet have acknowledged them).
    pub total_acks_sent: u64,
    /// Cumulative count of broker-reported ACK failures (CommandAckResponse with error).
    pub total_acks_failed: u64,
    /// Cumulative count of messages diverted to the DLQ pending list because they exceeded
    /// the configured `max_redeliver_count`. Mirrors the Java client's "exceeded max
    /// redelivery" counter — useful for monitoring poison-pill rates.
    pub total_msgs_dead_lettered: u64,
    /// Cumulative count of chunked messages that have been fully reassembled and
    /// delivered to the user-facing queue. Single-chunk and batched messages don't count
    /// here. Useful for picking up on unexpected chunk traffic / monitoring chunking
    /// activity.
    pub total_chunked_msgs_received: u64,
    /// Optional negative-ack tracker. When configured via
    /// `SubscribeRequest::negative_ack_redelivery_delay`, calls to `Connection::negative_ack`
    /// stage the ids here and the redelivery fires on the next `handle_timeout` once the
    /// delay has elapsed. `None` means immediate redelivery (the default).
    pub nack_tracker: Option<NegativeAcksTracker>,
    /// Optional unacked-message tracker. When configured via
    /// `SubscribeRequest::ack_timeout`, every delivered message is recorded into a
    /// sliding-window bucket and re-delivered if no positive ack arrives within the
    /// configured window. Mirrors Java's `UnAckedMessageTracker`.
    pub unacked_tracker: Option<UnackedMessageTracker>,
    /// PIP-54 batch-ack tracker. Keyed by the batch's `(ledger_id, entry_id)`, value is
    /// the bitset of *still-unacked* positions (bit `i` set ⇒ position `i` is unacked).
    /// Populated on first delivery of any message in a batch, cleared once every position
    /// is acked. When a batched message is acked individually, the client sends a partial
    /// ack carrying this bitset so the broker knows not to advance the cursor past the
    /// batch until every position is acked.
    pub batch_ack_tracker: HashMap<(u64, u64), BatchAckEntry>,
    /// Optional ack-grouping tracker. When configured via
    /// `SubscribeRequest::ack_group_time`, the runtime's `Consumer::ack_grouped` family
    /// stages individual / cumulative acks here and the state machine flushes them as one
    /// coalesced `CommandAck` per group window. `None` keeps every ack synchronous (the
    /// default).
    pub ack_tracker: Option<crate::trackers::AckGroupingTracker>,
    /// PIP-4 decryption failure handling, mirrors Java
    /// `org.apache.pulsar.client.api.ConsumerCryptoFailureAction`. Default `Fail`. The
    /// runtime engine reads this via [`Self::crypto_failure_action`] when decryption fails
    /// to decide whether to propagate, drop, or surface the ciphertext.
    pub crypto_failure_action: crate::conn::CryptoFailureAction,
    /// Receive-latency histogram, in milliseconds. Recorded on each [`Self::pop_message`] call,
    /// measuring the wall-clock interval between [`IncomingMessage::arrived_at`] (the moment
    /// the consumer state machine queued the message) and the moment the user calls
    /// `pop_message` / `receive`. Mirrors the latency percentiles surfaced by Java
    /// `ConsumerStatsRecorder` (p50, p99, max). Three significant digits, auto-resizing.
    pub receive_latency_hist: hdrhistogram::Histogram<u64>,
    /// Highest message id whose ack the runtime has surfaced via
    /// [`crate::Connection::ack`] / `ack_grouped_individual` / `ack_grouped_cumulative`. Used by
    /// [`crate::Connection::rebuild_consumers`] to set the `start_message_id` on the replayed
    /// `CommandSubscribe` so the broker resumes from the post-ack position after a reconnect
    /// (avoids double-delivery of pre-reconnect messages). `None` until the first ack lands.
    pub last_acked_message_id: Option<MessageId>,
    /// Last rolling-window stats snapshot: `(msgs_at_snapshot, bytes_at_snapshot, taken_at)`.
    /// Updated by [`Self::record_rate_window`] to compute msgs/sec + bytes/sec rates.
    /// Mirrors Java `ConsumerStatsRecorder` rolling-window rate calculation. `None` until
    /// the first `record_rate_window` call.
    pub last_rate_snapshot: Option<(u64, u64, std::time::Instant)>,
    /// Most recent rolling-window rate: messages-per-second delivered, computed from the delta
    /// between the previous and current `record_rate_window` calls. `0.0` until the second
    /// snapshot lands. Mirrors Java `ConsumerStats#getRateMsgsReceived`.
    pub current_msgs_per_sec: f64,
    /// Most recent rolling-window rate: bytes-per-second delivered. Mirrors Java
    /// `ConsumerStats#getRateBytesReceived`.
    pub current_bytes_per_sec: f64,
    /// PIP-180 / ADR-0033 shadow-topic metadata. `None` for a regular consumer
    /// (the default — byte-identical receive path to v0.1.0). `Some(meta)` is
    /// injected by the runtime engine at subscribe time when the admin REST
    /// `getShadowTopics(source)` lookup resolves the consumer's topic as a
    /// shadow of another. The connection's receive dispatch (see
    /// [`crate::Connection::poll_event`]) reads this and emits the
    /// [`crate::event::ConnectionEvent::MessageReceivedFromShadow`] variant
    /// instead of the regular [`crate::event::ConnectionEvent::Message`]
    /// when the inbound entry's [`pb::MessageMetadata::replicated_from`] is
    /// also populated.
    pub shadow_metadata: Option<ShadowTopicMetadata>,
}

/// One entry in the PIP-54 batch-ack tracker. Tracks which positions inside a single
/// batch are still unacked.
#[derive(Debug, Clone)]
pub struct BatchAckEntry {
    /// Number of messages in the batch (`metadata.num_messages_in_batch`).
    pub batch_size: i32,
    /// Bitset of unacked positions packed little-endian into `u64`s. Bit `i % 64` of
    /// word `i / 64` represents position `i` in the batch; `1` means unacked.
    pub unacked: Vec<u64>,
}

impl BatchAckEntry {
    /// Construct a fresh entry for a batch of `batch_size` messages — every position
    /// starts as unacked.
    #[must_use]
    pub fn fresh(batch_size: i32) -> Self {
        let size = batch_size.max(0) as usize;
        let n_words = size.div_ceil(64);
        let mut unacked = vec![0u64; n_words];
        for i in 0..size {
            unacked[i / 64] |= 1u64 << (i % 64);
        }
        Self {
            batch_size,
            unacked,
        }
    }

    /// Clear the bit at `position`. Returns `true` once *every* position has been acked
    /// (bitset all-zero), which means the caller can drop this entry and send a "full"
    /// ack (no `ack_set`) so the broker advances the cursor past the batch.
    pub fn ack_position(&mut self, position: i32) -> bool {
        if position < 0 || position >= self.batch_size {
            return self.is_fully_acked();
        }
        let p = position as usize;
        if let Some(word) = self.unacked.get_mut(p / 64) {
            *word &= !(1u64 << (p % 64));
        }
        self.is_fully_acked()
    }

    /// `true` if every position in the batch has been acked.
    #[must_use]
    pub fn is_fully_acked(&self) -> bool {
        self.unacked.iter().all(|w| *w == 0)
    }

    /// Borrow the bitset as `i64` for protobuf encoding. Pulsar's wire format declares
    /// `ack_set` as a `repeated int64`; bit semantics are unchanged by the cast.
    #[must_use]
    pub fn ack_set_i64(&self) -> Vec<i64> {
        #[allow(clippy::cast_possible_wrap)]
        self.unacked.iter().map(|&w| w as i64).collect()
    }
}

/// Snapshot of cumulative consumer counters. Mirrors `org.apache.pulsar.client.api.ConsumerStats`
/// for the totals; rates are derived above this layer. Latency percentiles mirror the p50/p99/max
/// surfaced by Java `ConsumerStatsRecorder`.
#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_field_names)]
pub struct ConsumerStats {
    /// Cumulative count of logical messages delivered.
    pub total_msgs_received: u64,
    /// Cumulative payload bytes delivered.
    pub total_bytes_received: u64,
    /// Cumulative count of ACK requests issued.
    pub total_acks_sent: u64,
    /// Cumulative count of broker-reported ACK failures.
    pub total_acks_failed: u64,
    /// Cumulative count of messages routed to the DLQ pending list (exceeded max redelivery).
    pub total_msgs_dead_lettered: u64,
    /// Cumulative count of chunked messages fully reassembled and delivered.
    pub total_chunked_msgs_received: u64,
    /// 50th percentile receive latency, in milliseconds, computed from the consumer's
    /// `receive_latency_hist`. Zero when no message has been popped yet.
    pub receive_latency_p50_ms: u64,
    /// 99th percentile receive latency, in milliseconds.
    pub receive_latency_p99_ms: u64,
    /// Maximum observed receive latency, in milliseconds.
    pub receive_latency_max_ms: u64,
    /// Rolling per-second message-receive rate, computed from the delta between the two most
    /// recent [`ConsumerState::record_rate_window`] calls. `0.0` before the second snapshot
    /// lands. Mirrors Java `ConsumerStats#getRateMsgsReceived`.
    pub msgs_per_sec: f64,
    /// Rolling per-second byte-receive rate. Mirrors Java `ConsumerStats#getRateBytesReceived`.
    pub bytes_per_sec: f64,
}

#[derive(Debug)]
struct ChunkBuffer {
    expected_chunks: i32,
    received_chunks: i32,
    /// Partial payload accumulator. Chunks may arrive in order; out-of-order chunk arrival is
    /// not expected over a single connection (the broker dispatches in order), but if it does,
    /// the buffer is indexed by `chunk_id` to make reassembly robust.
    chunk_payloads: HashMap<i32, Bytes>,
    first_metadata: pb::MessageMetadata,
    first_chunk_message_id: Option<MessageId>,
    broker_entry_metadata: Option<pb::BrokerEntryMetadata>,
    redelivery_count: u32,
}

/// Outcome of feeding one `CommandMessage` to the consumer.
#[derive(Debug, Clone)]
pub enum DeliverOutcome {
    /// One or more logical messages were delivered into the consumer queue.
    Delivered {
        /// Number of [`IncomingMessage`]s now in the queue.
        count: usize,
    },
    /// The message was buffered as a chunk; no user-visible message yet.
    Buffered,
    /// The message was dropped (e.g. duplicate chunk).
    Dropped,
}

impl ConsumerState {
    /// Construct a new consumer.
    pub fn new(
        handle: ConsumerHandle,
        topic: String,
        subscription: String,
        receiver_queue_size: usize,
    ) -> Self {
        Self {
            handle,
            topic,
            subscription,
            consumer_name: None,
            receiver_queue_size,
            available_permits: 0,
            consumed_since_flow: 0,
            queue: VecDeque::new(),
            chunk_reassembly: HashMap::new(),
            pending_seek: None,
            receive_wakers: Slab::new(),
            closed: false,
            max_redeliver_count: 0,
            dead_letter_pending: Vec::new(),
            paused: false,
            reached_end_of_topic: false,
            total_msgs_received: 0,
            total_bytes_received: 0,
            total_acks_sent: 0,
            total_acks_failed: 0,
            total_msgs_dead_lettered: 0,
            total_chunked_msgs_received: 0,
            nack_tracker: None,
            unacked_tracker: None,
            batch_ack_tracker: HashMap::new(),
            ack_tracker: None,
            crypto_failure_action: crate::conn::CryptoFailureAction::Fail,
            // 3 significant digits, auto-resizing — same precision the Java client uses for its
            // ConsumerStatsRecorder.
            receive_latency_hist: hdrhistogram::Histogram::<u64>::new(3)
                .expect("hdrhistogram precision 3 is valid"),
            last_acked_message_id: None,
            last_rate_snapshot: None,
            current_msgs_per_sec: 0.0,
            current_bytes_per_sec: 0.0,
            shadow_metadata: None,
        }
    }

    /// PIP-180 / ADR-0033: install shadow-topic metadata on this consumer.
    ///
    /// Called by the runtime engine at subscribe time after the admin REST
    /// `getShadowTopics(source)` lookup resolves this consumer's topic as a
    /// shadow of `meta.source_topic`. Once set, the connection's receive
    /// dispatch emits [`crate::event::ConnectionEvent::MessageReceivedFromShadow`]
    /// for every inbound entry whose [`pb::MessageMetadata::replicated_from`]
    /// is populated, instead of the regular
    /// [`crate::event::ConnectionEvent::Message`].
    ///
    /// Sans-io: the metadata is supplied externally so `magnetar-proto` has
    /// no admin-REST dependency ([ADR-0004](../adr/0004-sans-io-protocol-core.md)).
    pub fn set_shadow_metadata(&mut self, meta: ShadowTopicMetadata) {
        self.shadow_metadata = Some(meta);
    }

    /// PIP-180 / ADR-0033: pure classifier — returns
    /// `Some((source_topic, source_message_id))` when this consumer is
    /// shadow-attached AND the inbound entry carries
    /// [`pb::MessageMetadata::replicated_from`]. Used by the connection's
    /// receive dispatch to pick between [`crate::event::ConnectionEvent::Message`]
    /// and [`crate::event::ConnectionEvent::MessageReceivedFromShadow`].
    ///
    /// Returns `None` (regular delivery) when:
    ///   * `shadow_metadata` is `None` (consumer is not subscribed to a shadow topic), or
    ///   * the inbound metadata has no `replicated_from` field (the entry was authored on this
    ///     topic, not replicated from elsewhere).
    #[must_use]
    pub fn classify_for_shadow(&self, message: &IncomingMessage) -> Option<(String, MessageId)> {
        let shadow = self.shadow_metadata.as_ref()?;
        message.metadata.replicated_from.as_ref()?;
        // The broker presents the source-topic ledger/entry pointers verbatim;
        // by the PIP-180 structural-equality contract ([`MessageId`]) the
        // shadow-side id IS the source-side id.
        Some((shadow.source_topic.clone(), message.message_id))
    }

    /// Take a rolling-window snapshot at `now`. On the first call, just records
    /// the baseline and returns. On subsequent calls, computes the per-second
    /// delivery rates against the previous snapshot and writes them to
    /// [`Self::current_msgs_per_sec`] / [`Self::current_bytes_per_sec`].
    ///
    /// Sans-io discipline: `now` is injected (see
    /// [ADR-0011](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0011-clock-injection-sans-io.md)).
    /// Runtime engines typically wire this to a `tokio::time::interval` ticker.
    pub fn record_rate_window(&mut self, now: std::time::Instant) {
        if let Some((prev_msgs, prev_bytes, prev_at)) = self.last_rate_snapshot {
            let elapsed = now.saturating_duration_since(prev_at).as_secs_f64();
            if elapsed > f64::EPSILON {
                // The lossy cast is intentional — rates are reported as f64 (Java's `double`)
                // and ±1 unit on a u64 counter is irrelevant once you divide by seconds.
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "rate counters fit comfortably below f64::MAX_SAFE_INTEGER in practice"
                )]
                let d_msgs = self.total_msgs_received.saturating_sub(prev_msgs) as f64;
                #[allow(clippy::cast_precision_loss, reason = "same as above")]
                let d_bytes = self.total_bytes_received.saturating_sub(prev_bytes) as f64;
                self.current_msgs_per_sec = d_msgs / elapsed;
                self.current_bytes_per_sec = d_bytes / elapsed;
            }
        }
        self.last_rate_snapshot = Some((self.total_msgs_received, self.total_bytes_received, now));
    }

    /// PIP-4 decryption failure handling configured for this consumer. Mirrors Java
    /// `Consumer#getCryptoFailureAction`.
    #[must_use]
    pub fn crypto_failure_action(&self) -> crate::conn::CryptoFailureAction {
        self.crypto_failure_action
    }

    /// Snapshot of cumulative counters. Mirrors Java `ConsumerStats`.
    ///
    /// Latency percentiles (`receive_latency_*_ms`) are computed from the consumer's
    /// [`Self::receive_latency_hist`] at snapshot time so callers receive plain `u64` values
    /// without paying the histogram's clone cost. An empty histogram (no `pop_message` yet)
    /// yields zero percentiles.
    pub fn stats(&self) -> ConsumerStats {
        let p50 = self.receive_latency_p50_ms();
        let p99 = self.receive_latency_p99_ms();
        let pmax = self.receive_latency_max_ms();
        ConsumerStats {
            total_msgs_received: self.total_msgs_received,
            total_bytes_received: self.total_bytes_received,
            total_acks_sent: self.total_acks_sent,
            total_acks_failed: self.total_acks_failed,
            total_msgs_dead_lettered: self.total_msgs_dead_lettered,
            total_chunked_msgs_received: self.total_chunked_msgs_received,
            receive_latency_p50_ms: p50,
            receive_latency_p99_ms: p99,
            receive_latency_max_ms: pmax,
            msgs_per_sec: self.current_msgs_per_sec,
            bytes_per_sec: self.current_bytes_per_sec,
        }
    }

    /// 50th percentile receive latency, in milliseconds. Mirrors Java
    /// `ConsumerStatsRecorder#getRcvLatencyMillis50pct`.
    #[must_use]
    pub fn receive_latency_p50_ms(&self) -> u64 {
        if self.receive_latency_hist.is_empty() {
            return 0;
        }
        self.receive_latency_hist.value_at_quantile(0.50)
    }

    /// 99th percentile receive latency, in milliseconds. Mirrors Java
    /// `ConsumerStatsRecorder#getRcvLatencyMillis99pct`.
    #[must_use]
    pub fn receive_latency_p99_ms(&self) -> u64 {
        if self.receive_latency_hist.is_empty() {
            return 0;
        }
        self.receive_latency_hist.value_at_quantile(0.99)
    }

    /// Maximum observed receive latency, in milliseconds. Mirrors Java
    /// `ConsumerStatsRecorder#getRcvLatencyMillisMax`.
    #[must_use]
    pub fn receive_latency_max_ms(&self) -> u64 {
        if self.receive_latency_hist.is_empty() {
            return 0;
        }
        self.receive_latency_hist.max()
    }

    /// Returns a `CommandFlow` if the consumer is below half of its receiver queue and not in
    /// a frozen state. Resets the consumed counter. While [`Self::paused`] is `true` no flow
    /// is emitted — the broker stops dispatching once permits drain.
    pub fn maybe_flow(&mut self) -> Option<pb::CommandFlow> {
        if self.closed || self.pending_seek.is_some() || self.paused {
            return None;
        }
        let threshold = (self.receiver_queue_size / 2).max(1) as u32;
        if self.consumed_since_flow < threshold {
            return None;
        }
        let permits = self.consumed_since_flow;
        self.consumed_since_flow = 0;
        self.available_permits = self.available_permits.saturating_add(permits);
        Some(pb::CommandFlow {
            consumer_id: self.handle.0,
            message_permits: permits,
        })
    }

    /// Account for one broker-side ledger entry that the conn-level filter has decided to
    /// drop before reaching the user (PIP-33 replicated-subscription markers; any future
    /// drop-on-receive sentinel). The broker consumed one permit when it dispatched the
    /// entry, so we bump the internal `consumed_since_flow` counter symmetrically —
    /// otherwise the permit counter would drift after every marker and the broker would
    /// eventually stop dispatching.
    ///
    /// Intentionally **does not** increment the user-visible `total_msgs_received` /
    /// `total_bytes_received` counters: markers are not user messages.
    pub fn record_marker_consumed(&mut self) {
        self.consumed_since_flow = self.consumed_since_flow.saturating_add(1);
    }

    /// Force an initial flow for the configured receiver queue.
    pub fn initial_flow(&mut self) -> pb::CommandFlow {
        let permits = self.receiver_queue_size as u32;
        self.available_permits = permits;
        self.consumed_since_flow = 0;
        pb::CommandFlow {
            consumer_id: self.handle.0,
            message_permits: permits,
        }
    }

    /// Pop the next available message for the user. Caller wakes its future when a new message
    /// is delivered (the [`Connection`](crate::Connection) does this automatically).
    ///
    /// Records the wall-clock latency (`Instant::now() - msg.arrived_at`) into
    /// [`Self::receive_latency_hist`] so [`ConsumerStats`] can surface p50/p99/max.
    pub fn pop_message(&mut self) -> Option<IncomingMessage> {
        let msg = self.queue.pop_front()?;
        self.consumed_since_flow = self.consumed_since_flow.saturating_add(1);
        let latency_ms = u64::try_from(msg.arrived_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.receive_latency_hist.saturating_record(latency_ms);
        Some(msg)
    }

    /// Number of messages waiting to be popped.
    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    /// Feed one inbound `CommandMessage` + payload region. Handles batch explosion + chunk
    /// reassembly + DLQ flagging.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::Closed`] if the consumer has been closed.
    pub fn deliver(
        &mut self,
        cmd: &pb::CommandMessage,
        metadata: pb::MessageMetadata,
        broker_entry_metadata: Option<pb::BrokerEntryMetadata>,
        body: Bytes,
        now: std::time::Instant,
    ) -> Result<DeliverOutcome, ConsumerError> {
        if self.closed {
            return Err(ConsumerError::Closed);
        }
        // Java's `duringSeek` flag (apache/pulsar PR #21945, Jan 2024): while
        // a seek is in flight (we've sent CommandSeek but haven't yet seen
        // its CommandSuccess) the broker can keep dispatching pre-seek
        // messages that were already in its TCP send buffer. Those are
        // stale relative to the user's seek intent — they were dispatched
        // by the **old** cursor position, not the seek target. Drop them
        // here so they never reach the user-facing receive() and the
        // post-seek backlog is the only content the consumer sees.
        if self.pending_seek.is_some() {
            return Ok(DeliverOutcome::Dropped);
        }
        let redelivery = cmd.redelivery_count.unwrap_or(0);
        let mut message_id = MessageId::from_pb(&cmd.message_id);

        // Chunked message path.
        if let (Some(total), Some(chunk_id)) = (metadata.num_chunks_from_msg, metadata.chunk_id) {
            if total > 1 {
                let uuid = metadata.uuid.clone().unwrap_or_default();
                let entry = self
                    .chunk_reassembly
                    .entry(uuid.clone())
                    .or_insert_with(|| ChunkBuffer {
                        expected_chunks: total,
                        received_chunks: 0,
                        chunk_payloads: HashMap::new(),
                        first_metadata: metadata.clone(),
                        first_chunk_message_id: Some(message_id),
                        broker_entry_metadata: broker_entry_metadata.clone(),
                        redelivery_count: redelivery,
                    });
                if entry
                    .chunk_payloads
                    .insert(chunk_id, body.clone())
                    .is_some()
                {
                    return Ok(DeliverOutcome::Dropped);
                }
                entry.received_chunks += 1;
                if entry.received_chunks < entry.expected_chunks {
                    return Ok(DeliverOutcome::Buffered);
                }
                // All chunks present — assemble.
                let mut full = bytes::BytesMut::new();
                for idx in 0..entry.expected_chunks {
                    if let Some(chunk) = entry.chunk_payloads.remove(&idx) {
                        full.extend_from_slice(&chunk);
                    }
                }
                let assembled = full.freeze();
                let mut final_meta = entry.first_metadata.clone();
                final_meta.num_chunks_from_msg = None;
                final_meta.chunk_id = None;
                final_meta.total_chunk_msg_size = None;
                let first_chunk_message_id = entry.first_chunk_message_id;
                let bem = entry.broker_entry_metadata.clone();
                let redelivery_count = entry.redelivery_count;
                self.chunk_reassembly.remove(&uuid);

                // The "logical" message id is the *last* chunk's id (per Java
                // `ChunkMessageIdImpl.getLastChunkMessageId`). first_chunk_message_id is
                // already stored above; if the runtime needs it for ack, it should plumb it
                // via metadata properties.
                let _ = first_chunk_message_id;

                let im = IncomingMessage {
                    message_id,
                    metadata: final_meta,
                    single_metadata: None,
                    payload: assembled,
                    redelivery_count,
                    broker_entry_metadata: bem,
                    arrived_at: now,
                };
                self.total_chunked_msgs_received =
                    self.total_chunked_msgs_received.saturating_add(1);
                let trigger = self.classify_and_queue(im, redelivery_count, now);
                self.wake_receivers();
                return Ok(trigger);
            }
        }

        // Batched message path.
        let num_in_batch = metadata.num_messages_in_batch.unwrap_or(1);
        if num_in_batch > 1 {
            // PIP-54: stamp the per-batch ack tracker once. Subsequent acks of individual
            // positions in this batch clear bits in the bitset; the broker sees the partial
            // ack state and only advances the cursor once every position is acked.
            self.batch_ack_tracker
                .entry((message_id.ledger_id, message_id.entry_id))
                .or_insert_with(|| BatchAckEntry::fresh(num_in_batch));
            let mut cursor = body;
            let mut delivered = 0usize;
            for idx in 0..num_in_batch {
                if cursor.remaining() < 4 {
                    break;
                }
                let single_size = cursor.get_u32() as usize;
                if cursor.remaining() < single_size {
                    break;
                }
                let single_bytes = cursor.split_to(single_size);
                let single = match pb::SingleMessageMetadata::decode(single_bytes) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let payload_size = single.payload_size as usize;
                if cursor.remaining() < payload_size {
                    break;
                }
                let payload = cursor.split_to(payload_size);
                let mut single_mid = message_id;
                single_mid.batch_index = idx;
                single_mid.batch_size = num_in_batch;
                let im = IncomingMessage {
                    message_id: single_mid,
                    metadata: metadata.clone(),
                    single_metadata: Some(single),
                    payload,
                    redelivery_count: redelivery,
                    broker_entry_metadata: broker_entry_metadata.clone(),
                    arrived_at: now,
                };
                self.classify_and_queue(im, redelivery, now);
                delivered += 1;
            }
            self.wake_receivers();
            return Ok(DeliverOutcome::Delivered { count: delivered });
        }

        // Default: a single, non-chunked, non-batched message.
        message_id.batch_index = -1;
        message_id.batch_size = 0;
        let im = IncomingMessage {
            message_id,
            metadata,
            single_metadata: None,
            payload: body,
            redelivery_count: redelivery,
            broker_entry_metadata,
            arrived_at: now,
        };
        let outcome = self.classify_and_queue(im, redelivery, now);
        self.wake_receivers();
        Ok(outcome)
    }

    /// Route an [`IncomingMessage`] to the queue or the DLQ pending list. Returns the
    /// `DeliverOutcome::Delivered` count. `now` is the caller-supplied monotonic
    /// timestamp used by the ack-timeout tracker so the sans-io state machine never
    /// reaches for its own clock.
    fn classify_and_queue(
        &mut self,
        msg: IncomingMessage,
        redelivery: u32,
        now: std::time::Instant,
    ) -> DeliverOutcome {
        let payload_len = msg.payload.len();
        if self.max_redeliver_count > 0 && redelivery > self.max_redeliver_count {
            self.total_msgs_dead_lettered = self.total_msgs_dead_lettered.saturating_add(1);
            self.dead_letter_pending.push(msg);
            DeliverOutcome::Buffered
        } else {
            self.total_msgs_received = self.total_msgs_received.saturating_add(1);
            self.total_bytes_received =
                self.total_bytes_received.saturating_add(payload_len as u64);
            // Track for ack-timeout-driven redelivery — backoff-aware when the consumer was
            // configured with a PIP-37 `AckTimeoutRedeliveryBackoff`. `now` is supplied by
            // the caller so the sans-io state machine never reads its own clock.
            if let Some(tracker) = self.unacked_tracker.as_mut() {
                tracker.add_with_redelivery_count(msg.message_id, msg.redelivery_count, now);
            }
            self.queue.push_back(msg);
            DeliverOutcome::Delivered {
                count: self.queue.len(),
            }
        }
    }

    /// Drain every parked receive waker and wake it. Called on message arrival,
    /// close, end-of-topic, and supervised reset. Drain-all (rather than wake-one)
    /// matches the fan-out semantic users expect: any number of concurrent
    /// `receive()` futures get re-polled, and the first one to acquire the
    /// connection lock pops the message; the others observe the empty queue and
    /// re-park themselves.
    fn wake_receivers(&mut self) {
        let wakers: Vec<Waker> = self.receive_wakers.drain().collect();
        for w in wakers {
            w.wake();
        }
    }

    /// Begin a seek operation. Freezes the receiver queue until [`Self::seek_acked`].
    pub fn begin_seek(&mut self, request_id: RequestId) {
        self.pending_seek = Some(request_id);
        // Drop buffered messages — the broker will resend from the new position.
        self.queue.clear();
    }

    /// Acknowledge a previously-issued seek. Returns the request id, if one was pending.
    pub fn seek_acked(&mut self) -> Option<RequestId> {
        self.pending_seek.take()
    }

    /// Register a waker that fires when a new message arrives, the consumer is
    /// closed, or end-of-topic is signaled. Returns a slab key that the caller
    /// MUST pass to [`Self::cancel_receive_waker`] if the future is dropped
    /// before observing the wake — otherwise the slab leaks the entry until the
    /// next drain.
    ///
    /// Multiple in-flight `receive()` futures on the same consumer register
    /// independent slots; arrival drains all of them.
    pub fn register_receive_waker(&mut self, waker: Waker) -> usize {
        self.receive_wakers.insert(waker)
    }

    /// Evict a previously-registered receive waker. Idempotent — a missing slot
    /// is a no-op (a concurrent wake may already have drained it).
    pub fn cancel_receive_waker(&mut self, slab_key: usize) {
        if self.receive_wakers.contains(slab_key) {
            self.receive_wakers.remove(slab_key);
        }
    }

    /// Mark the consumer closed. Wakes every parked receive future so they can
    /// observe the terminal state.
    pub fn close(&mut self) {
        self.closed = true;
        self.wake_receivers();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn metadata(num_in_batch: i32) -> pb::MessageMetadata {
        pb::MessageMetadata {
            producer_name: "p".to_owned(),
            sequence_id: 1,
            publish_time: 1_700_000_000,
            num_messages_in_batch: Some(num_in_batch),
            ..Default::default()
        }
    }

    fn message_cmd(redelivery: u32) -> pb::CommandMessage {
        pb::CommandMessage {
            consumer_id: 1,
            message_id: pb::MessageIdData {
                ledger_id: 1,
                entry_id: 1,
                ..Default::default()
            },
            redelivery_count: Some(redelivery),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }
    }

    #[test]
    fn flow_emits_initial_permits() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let f = c.initial_flow();
        assert_eq!(f.consumer_id, 1);
        assert_eq!(f.message_permits, 100);
        assert_eq!(c.available_permits, 100);
    }

    #[test]
    fn flow_refills_on_half_drain() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 4);
        let _ = c.initial_flow();
        // Deliver 2 messages and pop them — half drained.
        for _ in 0..2 {
            c.deliver(
                &message_cmd(0),
                metadata(1),
                None,
                Bytes::from_static(b"x"),
                std::time::Instant::now(),
            )
            .unwrap();
            let _ = c.pop_message();
        }
        let flow = c.maybe_flow().expect("flow at half drain");
        assert_eq!(flow.message_permits, 2);
    }

    #[test]
    fn single_message_lands_in_queue() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        let outcome = c
            .deliver(
                &message_cmd(0),
                metadata(1),
                None,
                Bytes::from_static(b"hi"),
                std::time::Instant::now(),
            )
            .unwrap();
        assert!(matches!(outcome, DeliverOutcome::Delivered { .. }));
        let msg = c.pop_message().unwrap();
        assert_eq!(msg.payload.as_ref(), b"hi");
    }

    #[test]
    fn batch_message_explodes() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();

        // Build a batch payload: two singles with their length-prefixed metadata.
        let mut buf = bytes::BytesMut::new();
        for payload in [b"a".as_ref(), b"bb".as_ref()] {
            let sm = pb::SingleMessageMetadata {
                payload_size: payload.len() as i32,
                ..Default::default()
            };
            let sm_len = sm.encoded_len();
            buf.extend_from_slice(&(sm_len as u32).to_be_bytes());
            sm.encode(&mut buf).unwrap();
            buf.extend_from_slice(payload);
        }

        let outcome = c
            .deliver(
                &message_cmd(0),
                metadata(2),
                None,
                buf.freeze(),
                std::time::Instant::now(),
            )
            .unwrap();
        match outcome {
            DeliverOutcome::Delivered { count } => assert_eq!(count, 2),
            other => panic!("expected Delivered(2), got {other:?}"),
        }
        let m1 = c.pop_message().unwrap();
        let m2 = c.pop_message().unwrap();
        assert_eq!(m1.message_id.batch_index, 0);
        assert_eq!(m2.message_id.batch_index, 1);
        assert_eq!(m1.payload.as_ref(), b"a");
        assert_eq!(m2.payload.as_ref(), b"bb");
    }

    #[test]
    fn chunks_reassemble_into_one_message() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();

        let make_chunk = |idx: i32, payload: &'static [u8]| {
            let mut meta = pb::MessageMetadata {
                producer_name: "p".to_owned(),
                sequence_id: 1,
                publish_time: 1_700_000_000,
                ..Default::default()
            };
            meta.num_chunks_from_msg = Some(3);
            meta.chunk_id = Some(idx);
            meta.uuid = Some("u-1".to_owned());
            meta.total_chunk_msg_size = Some(6);
            (meta, Bytes::from_static(payload))
        };

        for (meta, body) in [
            make_chunk(0, b"aa"),
            make_chunk(1, b"bb"),
            make_chunk(2, b"cc"),
        ] {
            let outcome = c
                .deliver(&message_cmd(0), meta, None, body, std::time::Instant::now())
                .unwrap();
            // The first two are buffered; the third triggers delivery.
            match outcome {
                DeliverOutcome::Buffered | DeliverOutcome::Delivered { .. } => {}
                other => panic!("unexpected outcome: {other:?}"),
            }
        }
        let msg = c.pop_message().expect("reassembled message");
        assert_eq!(msg.payload.as_ref(), b"aabbcc");
        assert_eq!(c.stats().total_chunked_msgs_received, 1);
    }

    #[test]
    fn dlq_routes_after_max_redelivery() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        c.max_redeliver_count = 2;
        let _ = c.initial_flow();
        let _ = c
            .deliver(
                &message_cmd(5),
                metadata(1),
                None,
                Bytes::from_static(b"hi"),
                std::time::Instant::now(),
            )
            .unwrap();
        assert!(c.queue.is_empty());
        assert_eq!(c.dead_letter_pending.len(), 1);
    }

    #[test]
    fn consumer_stats_count_delivered_messages_only() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        let _ = c
            .deliver(
                &message_cmd(0),
                metadata(1),
                None,
                Bytes::from_static(b"hi"),
                std::time::Instant::now(),
            )
            .unwrap();
        let _ = c
            .deliver(
                &message_cmd(0),
                metadata(1),
                None,
                Bytes::from_static(b"hello"),
                std::time::Instant::now(),
            )
            .unwrap();
        let stats = c.stats();
        assert_eq!(stats.total_msgs_received, 2);
        assert_eq!(stats.total_bytes_received, 2 + 5);

        // DLQ-routed messages should not bump the received counter.
        c.max_redeliver_count = 2;
        let _ = c
            .deliver(
                &message_cmd(5),
                metadata(1),
                None,
                Bytes::from_static(b"DROPPED"),
                std::time::Instant::now(),
            )
            .unwrap();
        assert_eq!(c.stats().total_msgs_received, 2);
    }

    #[test]
    fn dlq_counter_increments_per_diverted_message() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        c.max_redeliver_count = 2;
        let _ = c.initial_flow();
        assert_eq!(c.stats().total_msgs_dead_lettered, 0);
        for _ in 0..3 {
            let _ = c
                .deliver(
                    &message_cmd(5),
                    metadata(1),
                    None,
                    Bytes::from_static(b"poison"),
                    std::time::Instant::now(),
                )
                .unwrap();
        }
        assert_eq!(c.stats().total_msgs_dead_lettered, 3);
        assert_eq!(c.dead_letter_pending.len(), 3);
    }

    #[test]
    fn batch_ack_entry_fresh_sets_all_unacked_bits() {
        let e = BatchAckEntry::fresh(5);
        // 5 bits set in the low word.
        assert_eq!(e.unacked, vec![0b0001_1111]);
        assert!(!e.is_fully_acked());
    }

    #[test]
    fn batch_ack_entry_acks_one_at_a_time() {
        let mut e = BatchAckEntry::fresh(3);
        assert!(!e.ack_position(0)); // 0b110 left
        assert_eq!(e.unacked, vec![0b110]);
        assert!(!e.ack_position(1)); // 0b100 left
        assert_eq!(e.unacked, vec![0b100]);
        assert!(e.ack_position(2)); // all acked
        assert!(e.is_fully_acked());
    }

    #[test]
    fn batch_ack_entry_spans_multiple_words() {
        let mut e = BatchAckEntry::fresh(70);
        assert_eq!(e.unacked.len(), 2);
        // Ack position 65 — clears bit 1 of word 1.
        assert!(!e.ack_position(65));
        assert_eq!(e.unacked[1] & (1 << 1), 0);
        assert!(!e.is_fully_acked());
    }

    #[test]
    fn batch_ack_entry_ignores_out_of_range_positions() {
        let mut e = BatchAckEntry::fresh(4);
        // -1 / >= batch_size are no-ops.
        let _ = e.ack_position(-1);
        let _ = e.ack_position(99);
        assert!(!e.is_fully_acked());
        assert_eq!(e.unacked, vec![0b1111]);
    }

    /// Drive a synthetic distribution through `receive_latency_hist` and confirm the snapshot
    /// percentiles + accessors line up with the input. Mirrors the Java
    /// `ConsumerStatsRecorderTest#testGetLatencyPercentiles` smoke test.
    #[test]
    fn receive_latency_percentiles_reflect_recorded_samples() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        // Empty histogram — accessors and snapshot must report zero, not panic.
        assert_eq!(c.receive_latency_p50_ms(), 0);
        assert_eq!(c.receive_latency_p99_ms(), 0);
        assert_eq!(c.receive_latency_max_ms(), 0);
        let stats0 = c.stats();
        assert_eq!(stats0.receive_latency_p50_ms, 0);
        assert_eq!(stats0.receive_latency_p99_ms, 0);
        assert_eq!(stats0.receive_latency_max_ms, 0);

        // 100 samples uniformly in [1, 100].
        for v in 1u64..=100 {
            c.receive_latency_hist.saturating_record(v);
        }
        let p50 = c.receive_latency_p50_ms();
        let p99 = c.receive_latency_p99_ms();
        let pmax = c.receive_latency_max_ms();
        assert!((45..=55).contains(&p50), "expected p50 ~50 ms, got {p50}");
        assert!((95..=100).contains(&p99), "expected p99 ~99 ms, got {p99}");
        assert_eq!(pmax, 100, "max sample is 100 ms");

        let stats = c.stats();
        assert_eq!(stats.receive_latency_p50_ms, p50);
        assert_eq!(stats.receive_latency_p99_ms, p99);
        assert_eq!(stats.receive_latency_max_ms, pmax);
    }

    /// End-to-end: deliver a message, sleep briefly, pop it, observe the histogram now has one
    /// sample whose max reflects the sleep duration.
    #[test]
    fn pop_message_records_receive_latency() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        c.deliver(
            &message_cmd(0),
            metadata(1),
            None,
            Bytes::from_static(b"x"),
            std::time::Instant::now(),
        )
        .unwrap();
        assert!(c.receive_latency_hist.is_empty());

        std::thread::sleep(std::time::Duration::from_millis(2));
        let _msg = c.pop_message().expect("queued message");
        assert_eq!(c.receive_latency_hist.len(), 1);
        assert!(c.receive_latency_max_ms() >= 1);
        let stats = c.stats();
        assert_eq!(stats.receive_latency_max_ms, c.receive_latency_max_ms());
    }

    // ---------------------------------------------------------------------
    // PIP-37 chunk reassembly behavioural tests — backported from Java
    // `org.apache.pulsar.client.impl.ChunkMessageIdImplTest` and the
    // `ConsumerImpl` chunked-receive paths in
    // `org.apache.pulsar.client.impl.ConsumerImpl`. They drive the
    // `ChunkBuffer` logic in this module without touching the wire.
    // ---------------------------------------------------------------------

    /// Build a chunk metadata for a logical message of `total` chunks identified
    /// by `uuid`. `seq` is the per-message sequence id (constant across chunks),
    /// `chunk_id` is the 0-based index of this chunk.
    fn chunk_meta(uuid: &str, seq: u64, total: i32, chunk_id: i32) -> pb::MessageMetadata {
        pb::MessageMetadata {
            producer_name: "p".to_owned(),
            sequence_id: seq,
            publish_time: 1_700_000_000,
            uuid: Some(uuid.to_owned()),
            num_chunks_from_msg: Some(total),
            chunk_id: Some(chunk_id),
            total_chunk_msg_size: Some(0),
            ..Default::default()
        }
    }

    /// A `CommandMessage` whose broker-assigned `MessageIdData` carries the
    /// caller-supplied `entry_id` so chunk-buffer tests can distinguish each
    /// chunk's own broker id from the logical message's surfaced id.
    fn message_cmd_at(entry_id: u64, redelivery: u32) -> pb::CommandMessage {
        pb::CommandMessage {
            consumer_id: 1,
            message_id: pb::MessageIdData {
                ledger_id: 1,
                entry_id,
                ..Default::default()
            },
            redelivery_count: Some(redelivery),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }
    }

    /// A single-chunk message (`num_chunks_from_msg == 1`) must NOT engage the
    /// chunk reassembly buffer — it should be delivered immediately, just like
    /// the non-chunked path. Mirrors the Java consumer's `processMessageChunk`
    /// short-circuit when `totalChunks <= 1`.
    #[test]
    fn single_chunk_message_delivers_immediately() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();

        let meta = chunk_meta("u-single", 11, 1, 0);
        let outcome = c
            .deliver(
                &message_cmd_at(7, 0),
                meta,
                None,
                Bytes::from_static(b"only-chunk"),
                std::time::Instant::now(),
            )
            .unwrap();
        match outcome {
            DeliverOutcome::Delivered { count } => assert_eq!(count, 1),
            other => panic!("expected Delivered(1), got {other:?}"),
        }
        // No chunk reassembly state should be left dangling.
        assert!(
            c.chunk_reassembly.is_empty(),
            "single-chunk messages must not allocate ChunkBuffer entries"
        );
        // The "total chunked messages" counter only counts messages that go
        // through the reassembly path — a 1-chunk message shouldn't bump it.
        assert_eq!(c.stats().total_chunked_msgs_received, 0);

        let msg = c.pop_message().expect("immediate delivery");
        assert_eq!(msg.payload.as_ref(), b"only-chunk");
        // Reassembly metadata must be cleared on the user-visible message: the
        // consumer never lies about a single-chunk message being chunked.
        assert!(
            msg.metadata.num_chunks_from_msg.is_none()
                || msg.metadata.num_chunks_from_msg == Some(1)
        );
    }

    /// Multi-chunk message: the first N-1 chunks are buffered and produce no
    /// queue activity; the last chunk triggers reassembly and queues a single
    /// logical message whose payload is the concatenation in chunk-id order.
    /// Mirrors the Java consumer's `processMessageChunk` accumulator.
    #[test]
    fn multi_chunk_message_buffers_until_last_chunk() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();

        // Three chunks of a logical message identified by uuid "u-multi".
        let payloads: [&[u8]; 3] = [b"aaa", b"bbb", b"cccc"];
        for (idx, body) in payloads.iter().enumerate() {
            let meta = chunk_meta("u-multi", 42, 3, idx as i32);
            let outcome = c
                .deliver(
                    &message_cmd_at(100 + idx as u64, 0),
                    meta,
                    None,
                    Bytes::copy_from_slice(body),
                    std::time::Instant::now(),
                )
                .unwrap();
            if idx < 2 {
                // Intermediate chunks must be buffered, not delivered.
                assert!(
                    matches!(outcome, DeliverOutcome::Buffered),
                    "chunk {idx} should buffer, got {outcome:?}"
                );
                assert_eq!(c.queue_len(), 0, "no user-visible message yet");
            } else {
                // The last chunk surfaces exactly one logical message.
                match outcome {
                    DeliverOutcome::Delivered { count } => assert_eq!(count, 1),
                    other => panic!("last chunk must deliver, got {other:?}"),
                }
            }
        }

        // After the last chunk: exactly one message, fully reassembled, and the
        // per-uuid buffer is cleaned up.
        assert_eq!(c.queue_len(), 1);
        assert!(c.chunk_reassembly.is_empty());
        let msg = c.pop_message().expect("reassembled message");
        assert_eq!(msg.payload.as_ref(), b"aaabbbcccc");
        assert_eq!(c.stats().total_chunked_msgs_received, 1);
        // Reassembled message must not carry chunk markers downstream.
        assert!(msg.metadata.chunk_id.is_none());
        assert!(msg.metadata.num_chunks_from_msg.is_none());
        assert!(msg.metadata.total_chunk_msg_size.is_none());
    }

    /// Out-of-order chunk arrival (chunk 2 before chunk 1) must still reassemble
    /// into the correct payload because the buffer is keyed by `chunk_id`.
    /// Although the broker normally dispatches chunks in order, reconnection
    /// races and replay can interleave them — the buffer logic is defensive.
    #[test]
    fn out_of_order_chunks_are_buffered_correctly() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();

        // Deliver chunk 2 first, then chunk 0, then chunk 1.
        let order: [(i32, &[u8]); 3] = [(2, b"ZZZZ"), (0, b"AAAA"), (1, b"BBBB")];
        for &(chunk_id, body) in &order {
            let meta = chunk_meta("u-oo", 99, 3, chunk_id);
            let outcome = c
                .deliver(
                    &message_cmd_at(200 + chunk_id as u64, 0),
                    meta,
                    None,
                    Bytes::copy_from_slice(body),
                    std::time::Instant::now(),
                )
                .unwrap();
            // The outcome on each delivery depends on whether the buffer is
            // complete; we just check the queue state at the end.
            let _ = outcome;
        }
        assert_eq!(c.queue_len(), 1, "all chunks present, one logical message");
        let msg = c.pop_message().expect("reassembled");
        // Reassembled in chunk-id order regardless of arrival order.
        assert_eq!(msg.payload.as_ref(), b"AAAABBBBZZZZ");
        assert!(c.chunk_reassembly.is_empty());
    }

    /// Duplicate chunk delivery (same uuid + chunk_id) must be a no-op — the
    /// reassembly buffer drops the duplicate and reports `Dropped` rather than
    /// double-counting it as progress. Mirrors the Java
    /// `processMessageChunk` guard against duplicate chunk delivery.
    #[test]
    fn duplicate_chunk_is_dropped() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();

        // First arrival of chunk 0/3 — should buffer.
        let m0 = chunk_meta("u-dup", 1, 3, 0);
        let outcome0 = c
            .deliver(
                &message_cmd_at(300, 0),
                m0,
                None,
                Bytes::from_static(b"first"),
                std::time::Instant::now(),
            )
            .unwrap();
        assert!(matches!(outcome0, DeliverOutcome::Buffered));

        // Second arrival of the SAME chunk_id 0/3 — must be dropped, the
        // received_chunks counter must NOT advance, and the buffered payload
        // must NOT be overwritten.
        let m0_dup = chunk_meta("u-dup", 1, 3, 0);
        let outcome_dup = c
            .deliver(
                &message_cmd_at(301, 0),
                m0_dup,
                None,
                Bytes::from_static(b"second"),
                std::time::Instant::now(),
            )
            .unwrap();
        assert!(
            matches!(outcome_dup, DeliverOutcome::Dropped),
            "duplicate chunk_id must be Dropped, got {outcome_dup:?}"
        );
        // Sanity: still one chunk seen, two more remaining.
        let entry = c
            .chunk_reassembly
            .get("u-dup")
            .expect("buffer still present");
        assert_eq!(entry.received_chunks, 1);
        assert_eq!(entry.expected_chunks, 3);
    }

    /// Chunks belonging to two different logical messages (different uuids)
    /// must be tracked independently. Interleaved arrival of chunks from
    /// message A and message B must still produce two separately reassembled
    /// messages once each set is complete.
    #[test]
    fn interleaved_chunked_messages_are_independent() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();

        // Interleaved arrival: A0, B0, A1, B1.
        let plan: [(&str, u64, i32, &[u8]); 4] = [
            ("u-A", 10, 0, b"A0"),
            ("u-B", 20, 0, b"B0"),
            ("u-A", 10, 1, b"A1"),
            ("u-B", 20, 1, b"B1"),
        ];
        for &(uuid, seq, chunk_id, body) in &plan {
            let meta = chunk_meta(uuid, seq, 2, chunk_id);
            let _ = c
                .deliver(
                    &message_cmd_at(400 + chunk_id as u64, 0),
                    meta,
                    None,
                    Bytes::copy_from_slice(body),
                    std::time::Instant::now(),
                )
                .unwrap();
        }
        // Both messages should be queued and the reassembly buffer empty.
        assert_eq!(c.queue_len(), 2);
        assert!(c.chunk_reassembly.is_empty());
        assert_eq!(c.stats().total_chunked_msgs_received, 2);

        // First popped: message A (queued first when its last chunk arrived).
        let a = c.pop_message().expect("A");
        let b = c.pop_message().expect("B");
        assert_eq!(a.payload.as_ref(), b"A0A1");
        assert_eq!(b.payload.as_ref(), b"B0B1");
    }

    // ---------------------------------------------------------------------
    // ChunkMessageId comparison semantics — backported from Java
    // `ChunkMessageIdImplTest`. The Java client exposes
    // `ChunkMessageIdImpl(firstChunkMessageId, lastChunkMessageId)` whose
    // ordering / equality is delegated to its `lastChunkMessageId`. Our
    // `MessageId` is the single user-facing id; the reassembled logical
    // message carries the *last* chunk's id (`ChunkMessageIdImpl
    // #getLastChunkMessageId`). The Java tests of compareTo/equals/hashCode
    // therefore map onto MessageId's derived Ord/Eq/Hash, which we exercise
    // here.
    // ---------------------------------------------------------------------

    /// Mirrors Java `ChunkMessageIdImplTest#compareToTest`.
    #[test]
    fn chunk_message_id_compare_semantics() {
        // chunkMsgId1 := (first=0/0/0, last=1/1/1) — its "logical" id is the last.
        let id1 = MessageId {
            ledger_id: 1,
            entry_id: 1,
            partition: 1,
            batch_index: -1,
            batch_size: 0,
        };
        // chunkMsgId2 := (first=2/2/2, last=3/3/3) — its "logical" id is 3/3/3.
        let id2 = MessageId {
            ledger_id: 3,
            entry_id: 3,
            partition: 3,
            batch_index: -1,
            batch_size: 0,
        };
        use core::cmp::Ordering;
        assert_eq!(id1.cmp(&id2), Ordering::Less);
        assert_eq!(id2.cmp(&id1), Ordering::Greater);
        assert_eq!(id2.cmp(&id2), Ordering::Equal);
    }

    /// Mirrors Java `ChunkMessageIdImplTest#equalsTest` + `hashCodeTest`. The
    /// Java client makes `equals` compare against the inner
    /// `lastChunkMessageId`, which means a plain `MessageIdImpl` carrying the
    /// same ledger/entry/partition as the chunked id's last chunk compares
    /// equal. We mirror that by checking that two `MessageId`s with the
    /// same field values are `Eq` and share a hash.
    #[test]
    fn chunk_message_id_equals_and_hash_semantics() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let logical_id_of_chunk1 = MessageId {
            ledger_id: 1,
            entry_id: 1,
            partition: 1,
            batch_index: -1,
            batch_size: 0,
        };
        let logical_id_of_chunk2 = MessageId {
            ledger_id: 3,
            entry_id: 3,
            partition: 3,
            batch_index: -1,
            batch_size: 0,
        };

        // A plain `MessageId` matching the lastChunkMessageId of chunk1.
        let plain = MessageId {
            ledger_id: 1,
            entry_id: 1,
            partition: 1,
            batch_index: -1,
            batch_size: 0,
        };
        // Equal to itself.
        assert_eq!(logical_id_of_chunk1, logical_id_of_chunk1);
        // Different chunks are unequal.
        assert_ne!(logical_id_of_chunk1, logical_id_of_chunk2);
        // A plain message id compares equal to the chunked id's last-chunk id.
        assert_eq!(plain, logical_id_of_chunk1);

        // Hash discipline: equal values hash equal; distinct values *probably*
        // don't (we just check the test data picks distinct hashes — the
        // derived `Hash` is structural).
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        logical_id_of_chunk1.hash(&mut h1);
        logical_id_of_chunk2.hash(&mut h2);
        assert_ne!(h1.finish(), h2.finish());
    }

    #[test]
    fn record_rate_window_baseline_then_delta() {
        let mut c = ConsumerState::new(
            crate::types::ConsumerHandle(1),
            "t".to_owned(),
            "s".to_owned(),
            10,
        );
        let t0 = std::time::Instant::now();

        // First call records the baseline; rates stay zero.
        c.total_msgs_received = 0;
        c.total_bytes_received = 0;
        c.record_rate_window(t0);
        assert!((c.current_msgs_per_sec - 0.0).abs() < f64::EPSILON);
        assert!((c.current_bytes_per_sec - 0.0).abs() < f64::EPSILON);
        assert!(c.last_rate_snapshot.is_some());

        // Simulate 100 messages / 1024 bytes received over 2 s — rates should
        // be 50 msgs/sec, 512 bytes/sec.
        c.total_msgs_received = 100;
        c.total_bytes_received = 1024;
        let t1 = t0 + std::time::Duration::from_secs(2);
        c.record_rate_window(t1);
        assert!((c.current_msgs_per_sec - 50.0).abs() < 0.001);
        assert!((c.current_bytes_per_sec - 512.0).abs() < 0.001);

        // Cumulative counters unchanged → next window snapshot reports zero rate.
        let t2 = t1 + std::time::Duration::from_secs(1);
        c.record_rate_window(t2);
        assert!((c.current_msgs_per_sec - 0.0).abs() < 0.001);
        assert!((c.current_bytes_per_sec - 0.0).abs() < 0.001);
    }

    #[test]
    fn record_rate_window_safe_under_zero_elapsed() {
        let mut c = ConsumerState::new(
            crate::types::ConsumerHandle(1),
            "t".to_owned(),
            "s".to_owned(),
            10,
        );
        let t0 = std::time::Instant::now();
        c.record_rate_window(t0);
        c.total_msgs_received = 100;
        // Repeat the snapshot at the same instant — should not divide by zero,
        // should leave the previous rate untouched.
        c.record_rate_window(t0);
        assert!(c.current_msgs_per_sec.is_finite());
    }

    /// Counter-backed `Wake` implementation used by the
    /// receive-waker-slab tests. `wake` and `wake_by_ref` both bump the
    /// underlying `AtomicUsize` so the tests can assert how many times
    /// the slab drained their waker.
    struct CountingWake(std::sync::atomic::AtomicUsize);

    impl std::task::Wake for CountingWake {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Build a [`std::task::Waker`] that increments a shared counter on
    /// every `wake` / `wake_by_ref` invocation, plus the counter itself
    /// so the test body can observe wake/cancel semantics without
    /// spinning up an executor.
    fn counting_waker() -> (std::task::Waker, std::sync::Arc<CountingWake>) {
        let inner = std::sync::Arc::new(CountingWake(std::sync::atomic::AtomicUsize::new(0)));
        let waker = std::task::Waker::from(std::sync::Arc::clone(&inner));
        (waker, inner)
    }

    #[test]
    fn receive_waker_slab_drains_on_message_delivery() {
        use std::sync::atomic::Ordering;

        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        let (w1, count1) = counting_waker();
        let (w2, count2) = counting_waker();
        let k1 = c.register_receive_waker(w1);
        let k2 = c.register_receive_waker(w2);
        assert_ne!(k1, k2, "each registration gets a distinct slab key");
        assert_eq!(c.receive_wakers.len(), 2);

        // Deliver a single message — both parked receivers should be woken,
        // and the slab should be drained.
        let outcome = c
            .deliver(
                &message_cmd(0),
                metadata(1),
                None,
                Bytes::from_static(b"hi"),
                std::time::Instant::now(),
            )
            .unwrap();
        assert!(matches!(outcome, DeliverOutcome::Delivered { .. }));
        assert_eq!(count1.0.load(Ordering::SeqCst), 1);
        assert_eq!(count2.0.load(Ordering::SeqCst), 1);
        assert_eq!(c.receive_wakers.len(), 0);

        // Subsequent cancel of already-drained keys is idempotent.
        c.cancel_receive_waker(k1);
        c.cancel_receive_waker(k2);
    }

    #[test]
    fn receive_waker_slab_drains_on_close() {
        use std::sync::atomic::Ordering;

        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let (w, count) = counting_waker();
        let _key = c.register_receive_waker(w);

        c.close();
        assert!(c.closed);
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert_eq!(c.receive_wakers.len(), 0);
    }

    #[test]
    fn receive_waker_slab_cancels_without_waking() {
        use std::sync::atomic::Ordering;

        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        let (w, count) = counting_waker();
        let key = c.register_receive_waker(w);

        // Cancel before any delivery — the waker must NOT be invoked.
        c.cancel_receive_waker(key);
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        assert_eq!(c.receive_wakers.len(), 0);

        // Subsequent deliveries with no parked wakers must not panic.
        let _ = c
            .deliver(
                &message_cmd(0),
                metadata(1),
                None,
                Bytes::from_static(b"hi"),
                std::time::Instant::now(),
            )
            .unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), 0);

        // Cancel of an already-cancelled key is idempotent.
        c.cancel_receive_waker(key);
    }

    #[test]
    fn receive_waker_slab_wakes_chunked_path_on_final_chunk() {
        // Regression: prior to the per-Recv waker fix, the chunked-message
        // path in `deliver` queued the reassembled message but did not
        // call `wake_receivers`, so parked receivers would only observe
        // the message on the next poll cycle. The fix invokes
        // `wake_receivers` on the chunked path, mirroring the
        // single-message and batched paths.
        use std::sync::atomic::Ordering;

        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        let (w, count) = counting_waker();
        let _key = c.register_receive_waker(w);

        let make_chunk = |idx: i32, payload: &'static [u8]| {
            let mut meta = pb::MessageMetadata {
                producer_name: "p".to_owned(),
                sequence_id: 1,
                publish_time: 1_700_000_000,
                ..Default::default()
            };
            meta.num_chunks_from_msg = Some(2);
            meta.chunk_id = Some(idx);
            meta.uuid = Some("u-wake".to_owned());
            meta.total_chunk_msg_size = Some(4);
            (meta, Bytes::from_static(payload))
        };
        for (meta, body) in [make_chunk(0, b"aa"), make_chunk(1, b"bb")] {
            let _ = c
                .deliver(&message_cmd(0), meta, None, body, std::time::Instant::now())
                .unwrap();
        }
        assert_eq!(
            count.0.load(Ordering::SeqCst),
            1,
            "chunked delivery must wake parked receivers"
        );
        let msg = c.pop_message().expect("reassembled message");
        assert_eq!(msg.payload.as_ref(), b"aabb");
    }

    // ---------- PIP-180 / ADR-0033: shadow-topic receive-side tests ----------

    fn shadow_im(ledger: u64, entry: u64, replicated_from: Option<&str>) -> IncomingMessage {
        let mut meta = pb::MessageMetadata {
            producer_name: "src-producer".to_owned(),
            sequence_id: 1,
            publish_time: 1_700_000_000,
            ..Default::default()
        };
        meta.replicated_from = replicated_from.map(str::to_owned);
        IncomingMessage {
            message_id: MessageId {
                ledger_id: ledger,
                entry_id: entry,
                partition: -1,
                batch_index: -1,
                batch_size: 0,
            },
            metadata: meta,
            single_metadata: None,
            payload: Bytes::from_static(b"payload"),
            redelivery_count: 0,
            broker_entry_metadata: None,
            arrived_at: std::time::Instant::now(),
        }
    }

    /// PIP-180: a shadow-attached consumer classifies a message carrying
    /// `MessageMetadata.replicated_from` as a shadow delivery, returning the
    /// source-topic name + source `MessageId`.
    #[test]
    fn consumer_classifies_shadow_via_metadata() {
        let mut c = ConsumerState::new(
            ConsumerHandle(1),
            "persistent://public/default/shadow-t".to_owned(),
            "s".to_owned(),
            100,
        );
        c.set_shadow_metadata(ShadowTopicMetadata {
            source_topic: "persistent://public/default/source-t".to_owned(),
        });
        let im = shadow_im(7, 42, Some("source-cluster"));
        let class = c
            .classify_for_shadow(&im)
            .expect("shadow consumer + replicated_from = shadow classification");
        assert_eq!(class.0, "persistent://public/default/source-t");
        // The source id is structurally equal to the shadow-side id (PIP-180
        // contract on `MessageId`).
        assert_eq!(class.1, im.message_id);
    }

    /// PIP-180: the connection's receive dispatch emits
    /// `MessageReceivedFromShadow` (not `Message`) when the consumer is
    /// shadow-attached AND the inbound entry carries `replicated_from`.
    /// Exercised here at the consumer level via `classify_for_shadow`; the
    /// conn.rs-level dispatch is the user of this classifier and is covered
    /// by the runtime integration tests.
    #[test]
    fn consumer_emits_message_received_from_shadow() {
        let mut c = ConsumerState::new(
            ConsumerHandle(1),
            "persistent://public/default/shadow-t".to_owned(),
            "s".to_owned(),
            100,
        );
        c.set_shadow_metadata(ShadowTopicMetadata {
            source_topic: "persistent://public/default/source-t".to_owned(),
        });
        // A message with `replicated_from` set — broker-presented shadow copy.
        let im = shadow_im(99, 1, Some("dc-east"));
        assert!(c.classify_for_shadow(&im).is_some());
        // Same consumer, message *without* `replicated_from` — falls back to
        // the regular `Message` event (e.g. a direct write to the shadow
        // topic, which PIP-180 disallows but defensive: classify still says
        // "regular").
        let im_no_repl = shadow_im(99, 2, None);
        assert!(
            c.classify_for_shadow(&im_no_repl).is_none(),
            "no `replicated_from` => regular Message event"
        );
    }

    /// PIP-180: a non-shadow consumer always classifies as regular —
    /// `MessageMetadata.replicated_from` on a non-shadow consumer (e.g. a
    /// geo-replicated topic that happens to carry the field) does NOT
    /// upgrade the delivery to a shadow event. The shadow path is opt-in
    /// via [`ConsumerState::set_shadow_metadata`].
    #[test]
    fn consumer_emits_message_received_for_non_shadow() {
        let c = ConsumerState::new(
            ConsumerHandle(1),
            "persistent://public/default/regular-t".to_owned(),
            "s".to_owned(),
            100,
        );
        // Consumer not configured with shadow metadata — even if the entry
        // carries `replicated_from` (e.g. geo-replicated topic), classify
        // returns None and the dispatch falls through to `Message`.
        let im = shadow_im(7, 42, Some("source-cluster"));
        assert!(c.classify_for_shadow(&im).is_none());
        // And the same regardless of `replicated_from`.
        let im_none = shadow_im(7, 43, None);
        assert!(c.classify_for_shadow(&im_none).is_none());
    }
}
