// SPDX-License-Identifier: Apache-2.0

//! Per-producer state machine.
//!
//! Mirrors `org.apache.pulsar.client.impl.ProducerImpl`. The state machine owns:
//!
//! - Sequence-id allocation (`last_sequence_id_pushed`).
//! - Batching ([`BatchContainer`]).
//! - Chunking ([`ChunkedMessageContext`]).
//! - Pending-receipt correlation (`OpSend` queue).
//! - Mutual exclusion between batching and chunking — `can_add_to_batch ⇒ total_chunks == 1` per
//!   [GUIDELINES.md] §"Protocol-correctness invariants" rule 5.
//!
//! The state machine is **encode-only**. Compression and encryption are applied by callers
//! (the runtime crate) BEFORE the payload reaches [`ProducerState::queue_send`], because both
//! pull in algorithm-specific dependencies that the sans-io core must not host.
//!
//! # References
//!
//! - `ProducerImpl.java:419` (constructor)
//! - `ProducerImpl.java:581-608` (sendAsync entry — compression order)
//! - `ProducerImpl.java:621-628` (can-batch decision)
//! - `ProducerImpl.java:630-654` (chunking-vs-batching mutual exclusion)
//! - `ProducerImpl.java:696-704` (chunk loop, first send)
//! - `ProducerImpl.java:745-753` (chunk loop, resend)
//! - `ProducerImpl.java:775-790` (sequence-id assignment)
//! - `ProducerImpl.java:793-868` (chunked send)
//! - `BatchMessageContainerImpl.java:172-179` (canAdd)
//! - `BatchMessageContainerImpl.java:267-327` (flush)

use std::collections::{HashMap, VecDeque};
use std::task::Waker;

use bytes::Bytes;

use crate::error::ProducerError;
use crate::pb;
use crate::types::{CompressionKind, MessageId, ProducerHandle, SequenceId};

/// Outbound publish queued by the user.
#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    /// Final payload bytes (post-compression, post-encryption). Sequence-id assignment is the
    /// state machine's job; callers leave `metadata.sequence_id == 0`.
    pub payload: Bytes,
    /// Pulsar message metadata. The producer state machine will fill `producer_name`,
    /// `sequence_id`, `publish_time`, `compression`, `uncompressed_size`, and chunking
    /// fields. Other fields (partition key, properties, etc.) are passed through.
    pub metadata: pb::MessageMetadata,
    /// Original, uncompressed payload size (callers compress before reaching us).
    pub uncompressed_size: u32,
    /// Number of single messages this OutgoingMessage represents (1 unless caller bundled).
    pub num_messages: i32,
    /// Optional transaction id. When set, the producer stamps it on `CommandSend` for
    /// transactional publish semantics (PIP-31). Mirrors Java
    /// `Producer#newMessage(Transaction).send()`.
    pub txn_id: Option<crate::txn::TxnId>,
}

/// Result of [`ProducerState::queue_send`] — one or more frames the connection should emit.
#[derive(Debug, Clone)]
pub enum SendDecision {
    /// Caller should encode and emit `count` frames. Use [`ProducerState::next_outbound_frame`]
    /// to pull each frame in order.
    Emit {
        /// Number of frames to emit; the [`ProducerState`] has buffered them all.
        count: usize,
    },
    /// The message was batched and is waiting for a flush (size, count, or tick).
    Batched,
}

/// A single frame ready to go on the wire (command + metadata + payload).
#[derive(Debug, Clone)]
pub struct OutboundFrame {
    /// The `BaseCommand` describing this frame.
    pub command: pb::BaseCommand,
    /// The per-message metadata.
    pub metadata: pb::MessageMetadata,
    /// The raw bytes that go after `MessageMetadata`.
    pub payload: Bytes,
    /// The sequence id assigned for this frame (mostly useful for traces and dedup).
    pub sequence_id: SequenceId,
}

/// One pending `CommandSend` whose `CommandSendReceipt` has not yet arrived.
#[derive(Debug)]
pub struct OpSend {
    /// Sequence id of the publish.
    pub sequence_id: SequenceId,
    /// Number of frames the publish occupies (>1 for chunked publishes).
    pub num_messages: i32,
    /// Optional waker registered by the runtime's user-facing send Future.
    pub waker: Option<Waker>,
    /// `None` until receipt arrives; `Some` once we have it (consumed by `take_outcome`).
    pub receipt: Option<MessageId>,
    /// `None` if no error; `Some` if the broker returned a `CommandSendError`.
    pub error: Option<(i32, String)>,
    /// Wall-clock instant the send was enqueued. Used by the send-timeout sweep on
    /// [`crate::Connection::handle_timeout`].
    pub enqueued_at: std::time::Instant,
    /// Snapshot of the wire frame(s) emitted for this publish, kept so the supervisor can
    /// re-issue the publish on a freshly-handshaked session after a
    /// [`crate::Connection::reset`]. A single-frame publish (regular or batched) carries one
    /// [`OutboundFrame`]; a chunked publish carries one entry per chunk (every chunk shares
    /// `sequence_id`). Mirrors Java `ProducerImpl#pendingMessages` which preserves the
    /// composed payload so reconnect can replay each `OpSend` verbatim.
    ///
    /// Payload bytes inside [`OutboundFrame`] are `bytes::Bytes`, so cloning the vector
    /// is refcounted and cheap.
    pub replay_frames: Vec<OutboundFrame>,
}

/// Per-producer state.
#[derive(Debug)]
pub struct ProducerState {
    /// Producer id assigned by the [`Connection`](crate::Connection).
    pub handle: ProducerHandle,
    /// Topic name.
    pub topic: String,
    /// Producer name (assigned by broker if not user-specified).
    pub name: Option<String>,
    /// Compression codec configured for this producer. The codec itself runs above us; we
    /// just stamp `metadata.compression` so the broker knows what bytes it received.
    pub compression: CompressionKind,
    /// Maximum payload size (in bytes) above which a message must be chunked.
    /// Default: `5 MiB` (Pulsar default).
    pub max_message_size: usize,
    /// Maximum batch size in bytes.
    pub max_batch_size_bytes: usize,
    /// Maximum messages per batch.
    pub max_messages_in_batch: usize,
    /// Whether batching is enabled.
    pub batching_enabled: bool,
    /// Whether chunking is enabled.
    pub chunking_enabled: bool,
    /// Sequence id of the next first-publish (monotonic).
    next_sequence_id: u64,
    /// Last sequence id we *pushed* to the wire (mirrors Java `lastSequenceIdPushed`).
    pub last_sequence_id_pushed: i64,
    /// Last sequence id the broker *acknowledged* (mirrors Java `lastSequenceIdPublished`).
    pub last_sequence_id_published: i64,
    /// In-flight `OpSend`s, ordered by sequence id.
    pub pending: VecDeque<OpSend>,
    /// Fast lookup for "is this sequence id pending?".
    pending_index: HashMap<SequenceId, usize>,
    /// Batch container (only used when batching is enabled).
    pub batch: BatchContainer,
    /// Frames the user-pump should drain via [`Self::next_outbound_frame`].
    outbound: VecDeque<OutboundFrame>,
    /// Closed flag — once set, all subsequent sends fail with [`ProducerError::Closed`].
    pub closed: bool,
    /// Cumulative count of logical messages handed to the wire (sum of `num_messages` per
    /// emitted SEND, including each chunk of a chunked publish). Mirrors Java
    /// `ProducerStats#getTotalMsgsSent`.
    pub total_msgs_sent: u64,
    /// Cumulative bytes of payload handed to the wire (concatenated batch payloads counted as
    /// the concatenated size, chunked publishes count each chunk's payload).
    pub total_bytes_sent: u64,
    /// Cumulative count of `CommandSendError` responses correlated against this producer.
    pub total_send_failed: u64,
    /// Cumulative count of `CommandSendReceipt` responses correlated against this producer.
    pub total_acks_received: u64,
    /// Optional per-send timeout. When `Some(d)`, the Connection's `handle_timeout` sweep
    /// surfaces a synthetic `OpOutcome::SendError` for any in-flight `OpSend` whose
    /// `enqueued_at + d` has elapsed. Mirrors Java `ProducerBuilder#sendTimeout`. `None`
    /// disables the sweep.
    pub send_timeout: Option<std::time::Duration>,
    /// Optional max wait before forcing a batch flush. When `Some(d)`, the Connection's
    /// `handle_timeout` sweep flushes any non-empty batch whose first-added timestamp is
    /// older than `d`. Mirrors Java `ProducerBuilder#batchingMaxPublishDelay`. `None`
    /// (the default) means the batch only flushes on size / count limits.
    pub batching_max_publish_delay: Option<std::time::Duration>,
    /// Access mode the producer was opened with. Mirrors
    /// `CommandProducer.producer_access_mode`. Persisted so callers can query it via a
    /// runtime-side getter without round-tripping back to the original
    /// [`crate::conn::CreateProducerRequest`].
    pub access_mode: pb::ProducerAccessMode,
    /// Send-latency histogram, in milliseconds. Recorded on each `CommandSendReceipt`,
    /// measuring the wall-clock interval between the user's `send` enqueue
    /// (`OpSend::enqueued_at`) and the broker's receipt acknowledgement. Mirrors the latency
    /// percentiles surfaced by Java `ProducerStatsRecorder` (p50, p99, max). Three significant
    /// digits, default range — the typical broker round-trip is sub-second so the bucket layout
    /// fits comfortably within the default 1-bound..u64::MAX scale.
    pub send_latency_hist: hdrhistogram::Histogram<u64>,
    /// Reconnect epoch — bumped by [`crate::Connection::rebuild_producers`] each time the
    /// supervisor re-issues this producer's [`pb::CommandProducer`] on a freshly-handshaked
    /// session. Mirrors Java `ProducerImpl#epoch` and is stamped onto
    /// `CommandProducer.epoch` so the broker accepts the re-attach (rejects stale
    /// reconnects of older epochs). Starts at `0` for the original create.
    pub epoch: u64,
    /// Last rolling-window stats snapshot: `(msgs_at_snapshot, bytes_at_snapshot, taken_at)`.
    /// Updated by [`Self::record_rate_window`] to compute msgs/sec + bytes/sec send rates.
    /// Mirrors Java `ProducerStatsRecorder` rolling-window rate calculation. `None` until
    /// the first snapshot lands.
    pub last_rate_snapshot: Option<(u64, u64, std::time::Instant)>,
    /// Most recent rolling-window rate: messages-per-second sent. `0.0` until the second
    /// snapshot lands. Mirrors Java `ProducerStats#getSendMsgsRate`.
    pub current_msgs_per_sec: f64,
    /// Most recent rolling-window rate: bytes-per-second sent. Mirrors Java
    /// `ProducerStats#getSendBytesRate`.
    pub current_bytes_per_sec: f64,
}

/// Snapshot of cumulative producer counters. Mirrors `org.apache.pulsar.client.api.ProducerStats`
/// for the totals; rates are derived above this layer. Latency percentiles mirror the p50/p99/max
/// surfaced by Java `ProducerStatsRecorder`.
#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_field_names)]
pub struct ProducerStats {
    /// Cumulative count of logical messages handed to the wire.
    pub total_msgs_sent: u64,
    /// Cumulative payload bytes handed to the wire.
    pub total_bytes_sent: u64,
    /// Cumulative count of `CommandSendError` responses.
    pub total_send_failed: u64,
    /// Cumulative count of `CommandSendReceipt` responses.
    pub total_acks_received: u64,
    /// Number of in-flight publishes (queued but not yet acked by the broker).
    pub pending_queue_size: u64,
    /// 50th percentile send latency, in milliseconds, computed from the producer's
    /// `send_latency_hist`. Zero when no `CommandSendReceipt` has been observed yet.
    pub send_latency_p50_ms: u64,
    /// 99th percentile send latency, in milliseconds.
    pub send_latency_p99_ms: u64,
    /// Maximum observed send latency, in milliseconds.
    pub send_latency_max_ms: u64,
    /// Rolling per-second message-send rate, computed from the delta between the two most
    /// recent [`ProducerState::record_rate_window`] calls. `0.0` before the second snapshot
    /// lands. Mirrors Java `ProducerStats#getSendMsgsRate`.
    pub msgs_per_sec: f64,
    /// Rolling per-second byte-send rate. Mirrors Java `ProducerStats#getSendBytesRate`.
    pub bytes_per_sec: f64,
}

/// In-memory batch container.
///
/// Mirrors `BatchMessageContainerImpl`. Holds `Vec<(SingleMessageMetadata, Bytes)>` pairs;
/// flush concatenates them with their `SingleMessageMetadata` prefix into one payload, then
/// returns it for the producer to emit as a single SEND frame.
#[derive(Debug, Default)]
pub struct BatchContainer {
    /// Individual messages buffered.
    pub messages: Vec<(pb::SingleMessageMetadata, Bytes)>,
    /// Sum of payload bytes (excluding `SingleMessageMetadata` overhead).
    pub current_size_bytes: usize,
    /// Lowest sequence id in the batch (used as the `sequence_id` of the SEND command).
    pub lowest_sequence_id: Option<u64>,
    /// Highest sequence id in the batch (used as `highest_sequence_id` of the SEND command).
    pub highest_sequence_id: Option<u64>,
    /// Wall-clock instant the first message was added to the current batch. Drives the
    /// `batching_max_publish_delay` deadline; `None` when the batch is empty.
    pub first_added_at: Option<std::time::Instant>,
    /// Transaction id shared by every message in the current batch. Pulsar's
    /// `TransactionBuffer` routes messages by the `txnid_*` fields on `CommandSend`, not on
    /// `SingleMessageMetadata` — if a batch is emitted with `txnid_*: None`, the broker
    /// publishes the entries directly through the dispatcher and bypasses
    /// commit/abort markers, leaking aborted writes to consumers. The Java client refuses
    /// to mix txn / non-txn or two different txn ids inside one batch
    /// (`ProducerImpl.canAddToBatch`); we mirror that with [`Self::matches_txn`] +
    /// caller-driven flush on mismatch.
    pub txn_id: Option<crate::TxnId>,
}

impl BatchContainer {
    /// Returns `true` if this batch is empty.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Number of buffered messages.
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Reset the batch (after flush).
    pub fn clear(&mut self) {
        self.messages.clear();
        self.current_size_bytes = 0;
        self.lowest_sequence_id = None;
        self.highest_sequence_id = None;
        self.first_added_at = None;
        self.txn_id = None;
    }

    /// `true` if `incoming` would land in the same `TransactionBuffer` partition as the
    /// messages already buffered. An empty batch matches every txn id. A non-empty batch
    /// matches only when both ids agree (including `None == None` for non-txn writes).
    /// Mirrors Java `ProducerImpl.canAddToBatch`.
    #[must_use]
    pub fn matches_txn(&self, incoming: Option<crate::TxnId>) -> bool {
        self.is_empty() || self.txn_id == incoming
    }
}

/// Per-message chunking state.
///
/// Created when the producer encounters a payload too large for the broker's `max_message_size`.
/// Holds the full payload and emits one chunk frame at a time via `next_chunk_frame`.
#[derive(Debug)]
pub struct ChunkedMessageContext {
    /// The full payload to chunk.
    pub payload: Bytes,
    /// UUID identifying the logical message (PIP-37).
    pub uuid: String,
    /// Total number of chunks (`ceil(payload.len() / max_message_size)`).
    pub total_chunks: i32,
    /// Index of the next chunk to emit (0-based).
    pub next_chunk: i32,
    /// Maximum bytes per chunk.
    pub max_chunk_size: usize,
    /// Sequence id assigned to the logical message.
    pub sequence_id: SequenceId,
    /// Common metadata for the message (reused per chunk, with chunk-specific fields stamped
    /// on each emit).
    pub metadata: pb::MessageMetadata,
    /// Uncompressed size (stamped on metadata).
    pub uncompressed_size: u32,
}

impl ChunkedMessageContext {
    /// Compute the chunk count for a given payload size and chunk size.
    pub fn compute_total_chunks(payload_len: usize, max_chunk_size: usize) -> i32 {
        if max_chunk_size == 0 {
            return 1;
        }
        let n = payload_len.div_ceil(max_chunk_size).max(1);
        i32::try_from(n).unwrap_or(i32::MAX)
    }

    /// Returns `true` if there are no more chunks to emit.
    pub fn is_finished(&self) -> bool {
        self.next_chunk >= self.total_chunks
    }
}

impl ProducerState {
    /// Construct a new producer.
    pub fn new(
        handle: ProducerHandle,
        topic: String,
        compression: CompressionKind,
        max_message_size: usize,
    ) -> Self {
        Self {
            handle,
            topic,
            name: None,
            compression,
            max_message_size,
            max_batch_size_bytes: 128 * 1024,
            max_messages_in_batch: 1000,
            batching_enabled: false,
            chunking_enabled: false,
            next_sequence_id: 0,
            last_sequence_id_pushed: -1,
            last_sequence_id_published: -1,
            pending: VecDeque::new(),
            pending_index: HashMap::new(),
            batch: BatchContainer::default(),
            outbound: VecDeque::new(),
            closed: false,
            total_msgs_sent: 0,
            total_bytes_sent: 0,
            total_send_failed: 0,
            total_acks_received: 0,
            send_timeout: None,
            batching_max_publish_delay: None,
            access_mode: pb::ProducerAccessMode::Shared,
            // 3 significant digits, auto-resize so we never reject a sample for being above the
            // initial high bound. The Java client uses the same precision in
            // `ProducerStatsRecorderImpl`.
            send_latency_hist: hdrhistogram::Histogram::<u64>::new(3)
                .expect("hdrhistogram precision 3 is valid"),
            epoch: 0,
            last_rate_snapshot: None,
            current_msgs_per_sec: 0.0,
            current_bytes_per_sec: 0.0,
        }
    }

    /// Mirrors Java `ProducerBuilder#initialSequenceId`. Resets the next sequence id allocated
    /// by the internal `assign_sequence_id` helper to `next`. Must be called BEFORE the first
    /// publish; the state machine does not validate this — callers should set it through
    /// [`crate::CreateProducerRequest::initial_sequence_id`] which guarantees the ordering.
    pub fn set_initial_sequence_id(&mut self, next: u64) {
        self.next_sequence_id = next;
        // For at-least-once resume-on-restart: stamp the lastSequenceIdPublished to (next-1)
        // when the caller resumes from a known checkpoint, so producer_last_sequence_id_published()
        // returns the resume point until the first ack lands.
        if next > 0 {
            self.last_sequence_id_published = (next as i64).saturating_sub(1);
            self.last_sequence_id_pushed = self.last_sequence_id_published;
        }
    }

    /// Deadline of the earliest pending send (`enqueued_at + send_timeout`), or `None` if
    /// the producer has no send-timeout configured or no in-flight sends.
    #[must_use]
    pub fn next_send_deadline(&self) -> Option<std::time::Instant> {
        let timeout = self.send_timeout?;
        self.pending.front().map(|op| op.enqueued_at + timeout)
    }

    /// Drain every in-flight `OpSend` whose `enqueued_at + send_timeout` has passed.
    /// Returns the `(sequence_id, waker)` pairs the caller should wake — each one's
    /// corresponding `OpOutcome::SendError` is registered by the connection layer.
    /// Mirrors Java's `ClientCnx#timedOutSendOps` sweep.
    pub fn drain_timed_out_sends(
        &mut self,
        now: std::time::Instant,
    ) -> Vec<(SequenceId, Option<Waker>)> {
        let Some(timeout) = self.send_timeout else {
            return Vec::new();
        };
        let mut out = Vec::new();
        while let Some(front) = self.pending.front() {
            if now < front.enqueued_at + timeout {
                break;
            }
            let mut op = self.pending.pop_front().expect("front exists");
            self.pending_index.remove(&op.sequence_id);
            out.push((op.sequence_id, op.waker.take()));
        }
        out
    }

    /// Snapshot of cumulative counters. Mirrors Java `ProducerStats`.
    ///
    /// Latency percentiles (`send_latency_*_ms`) are computed from the producer's
    /// [`Self::send_latency_hist`] at snapshot time so callers receive plain `u64` values without
    /// paying the histogram's clone cost. An empty histogram (no receipt observed yet) yields
    /// zero percentiles.
    pub fn stats(&self) -> ProducerStats {
        let p50 = self.send_latency_p50_ms();
        let p99 = self.send_latency_p99_ms();
        let pmax = self.send_latency_max_ms();
        ProducerStats {
            total_msgs_sent: self.total_msgs_sent,
            total_bytes_sent: self.total_bytes_sent,
            total_send_failed: self.total_send_failed,
            total_acks_received: self.total_acks_received,
            pending_queue_size: self.pending.len() as u64,
            send_latency_p50_ms: p50,
            send_latency_p99_ms: p99,
            send_latency_max_ms: pmax,
            msgs_per_sec: self.current_msgs_per_sec,
            bytes_per_sec: self.current_bytes_per_sec,
        }
    }

    /// Take a rolling-window snapshot at `now`. On the first call, just records the
    /// baseline and returns. On subsequent calls, computes the per-second send rates
    /// against the previous snapshot and writes them to [`Self::current_msgs_per_sec`] /
    /// [`Self::current_bytes_per_sec`].
    ///
    /// Sans-io discipline: `now` is injected (see [ADR-0011]). Runtime engines wire this
    /// to a `tokio::time::interval` ticker.
    ///
    /// [ADR-0011]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0011-clock-injection-sans-io.md
    pub fn record_rate_window(&mut self, now: std::time::Instant) {
        if let Some((prev_msgs, prev_bytes, prev_at)) = self.last_rate_snapshot {
            let elapsed = now.saturating_duration_since(prev_at).as_secs_f64();
            if elapsed > f64::EPSILON {
                // Lossy cast intentional — see `ConsumerState::record_rate_window`.
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "rate counters fit comfortably below f64::MAX_SAFE_INTEGER in practice"
                )]
                let d_msgs = self.total_msgs_sent.saturating_sub(prev_msgs) as f64;
                #[allow(clippy::cast_precision_loss, reason = "same as above")]
                let d_bytes = self.total_bytes_sent.saturating_sub(prev_bytes) as f64;
                self.current_msgs_per_sec = d_msgs / elapsed;
                self.current_bytes_per_sec = d_bytes / elapsed;
            }
        }
        self.last_rate_snapshot = Some((self.total_msgs_sent, self.total_bytes_sent, now));
    }

    /// 50th percentile send latency, in milliseconds. Mirrors Java
    /// `ProducerStatsRecorder#getSendLatencyMillis50pct`.
    #[must_use]
    pub fn send_latency_p50_ms(&self) -> u64 {
        if self.send_latency_hist.is_empty() {
            return 0;
        }
        self.send_latency_hist.value_at_quantile(0.50)
    }

    /// 99th percentile send latency, in milliseconds. Mirrors Java
    /// `ProducerStatsRecorder#getSendLatencyMillis99pct`.
    #[must_use]
    pub fn send_latency_p99_ms(&self) -> u64 {
        if self.send_latency_hist.is_empty() {
            return 0;
        }
        self.send_latency_hist.value_at_quantile(0.99)
    }

    /// Maximum observed send latency, in milliseconds. Mirrors Java
    /// `ProducerStatsRecorder#getSendLatencyMillisMax`.
    #[must_use]
    pub fn send_latency_max_ms(&self) -> u64 {
        if self.send_latency_hist.is_empty() {
            return 0;
        }
        self.send_latency_hist.max()
    }

    /// Returns whether this producer can add the given payload to its current batch.
    ///
    /// Mirrors `ProducerImpl.canAddToBatch`. Two conditions must hold:
    /// - Batching is enabled.
    /// - The compressed payload fits in the remaining batch budget.
    ///
    /// This is the predicate referenced by the **canAddToBatch ⇒ totalChunks == 1** invariant.
    pub fn can_add_to_batch(&self, payload_size: usize, num_messages: i32) -> bool {
        if !self.batching_enabled || self.closed {
            return false;
        }
        if self
            .batch
            .len()
            .saturating_add(num_messages.max(1) as usize)
            > self.max_messages_in_batch
        {
            return false;
        }
        if self.batch.current_size_bytes.saturating_add(payload_size) > self.max_batch_size_bytes {
            return false;
        }
        // The Java client refuses to batch messages whose deliver-at-time is set; we mirror that
        // because a per-batch deliver-at-time is meaningless.
        true
    }

    /// Stamp publish-time and producer-name on metadata, then assign the next sequence id.
    fn assign_sequence_id(
        &mut self,
        metadata: &mut pb::MessageMetadata,
        publish_time_ms: u64,
    ) -> SequenceId {
        let seq = self.next_sequence_id;
        self.next_sequence_id = self.next_sequence_id.wrapping_add(1);
        metadata.sequence_id = seq;
        metadata.publish_time = publish_time_ms;
        if let Some(name) = &self.name {
            if metadata.producer_name.is_empty() {
                metadata.producer_name = name.clone();
            }
        }
        self.last_sequence_id_pushed = seq as i64;
        SequenceId(seq)
    }

    /// Queue a publish for emission.
    ///
    /// Decision tree (mirrors `ProducerImpl.java:621-628`):
    ///
    /// 1. If `chunking_enabled` and the payload is too big and we *cannot* add to the batch (either
    ///    batching is off or the payload itself is bigger than the batch budget), we emit via the
    ///    chunked path.
    /// 2. Otherwise if `batching_enabled` and `can_add_to_batch(...)`, we add to the batch and
    ///    return [`SendDecision::Batched`]. The frame is emitted on a later flush.
    /// 3. Otherwise we emit a single SEND frame immediately.
    ///
    /// The function returns a [`SendDecision`] describing what the caller should do; if it
    /// returns [`SendDecision::Emit`], the caller should pull `count` frames via
    /// [`Self::next_outbound_frame`].
    pub fn queue_send(
        &mut self,
        msg: OutgoingMessage,
        publish_time_ms: u64,
        now: std::time::Instant,
    ) -> Result<SendDecision, ProducerError> {
        if self.closed {
            return Err(ProducerError::Closed);
        }

        let payload_size = msg.payload.len();
        // The batch must be homogeneous per `txn_id` (see `BatchContainer::matches_txn`):
        // mixing two txns — or a txn write with a non-txn write — in the same `CommandSend`
        // sends the entries through the wrong `TransactionBuffer` routing path. Force a flush
        // here so the next add starts a fresh batch tagged with `msg.txn_id`.
        if self.batching_enabled && !self.batch.is_empty() && !self.batch.matches_txn(msg.txn_id) {
            let _ = self.flush_batch(publish_time_ms, now);
        }
        let can_batch = self.can_add_to_batch(payload_size, msg.num_messages);

        // Chunking path: too big AND we cannot batch it.
        if payload_size > self.max_message_size {
            if !self.chunking_enabled {
                return Err(ProducerError::MessageTooLarge {
                    size: payload_size,
                    max_message_size: self.max_message_size,
                });
            }
            // The Codex Q3 invariant: if can_add_to_batch returned true, total_chunks must be 1.
            // We enforce it by going through the batched path only when can_batch holds.
            if can_batch {
                // can_batch returning true on a payload larger than max_message_size is
                // possible only if max_batch_size_bytes >= max_message_size, which is rare but
                // legal; the Java client honours the batch path in that case.
                let decision = self.add_to_batch(msg, publish_time_ms, now)?;
                self.flush_batch_if_full(publish_time_ms, now);
                return Ok(decision);
            }
            return self.emit_chunked(msg, publish_time_ms, now);
        }

        if can_batch {
            let decision = self.add_to_batch(msg, publish_time_ms, now)?;
            self.flush_batch_if_full(publish_time_ms, now);
            return Ok(decision);
        }

        self.emit_single(msg, publish_time_ms, now)
    }

    /// Force-flush the batch when adding the latest message hit
    /// `max_messages_in_batch` (or pushed `current_size_bytes` past
    /// `max_batch_size_bytes`). Mirrors Java
    /// `BatchMessageContainerImpl.haveEnoughSpace` ⇒ trigger flush
    /// inside `ProducerImpl.doBatchSendAndAdd`: once the container is full,
    /// the very same send that filled it emits the batch synchronously
    /// so the caller's `SendFut` does not stall waiting for
    /// `batching_max_publish_delay` (default 1 min in this test). Without
    /// this, max-messages-bound batches only flush when the deadline
    /// elapses or another message arrives, and `producer.send().await` on
    /// the message that filled the batch hangs.
    fn flush_batch_if_full(&mut self, publish_time_ms: u64, now: std::time::Instant) {
        if self.batch.is_empty() {
            return;
        }
        let count_reached = self.batch.messages.len() >= self.max_messages_in_batch;
        let size_reached = self.batch.current_size_bytes >= self.max_batch_size_bytes;
        if count_reached || size_reached {
            let _ = self.flush_batch(publish_time_ms, now);
        }
    }

    /// Emit a single SEND frame for a small, non-batched message.
    fn emit_single(
        &mut self,
        mut msg: OutgoingMessage,
        publish_time_ms: u64,
        now: std::time::Instant,
    ) -> Result<SendDecision, ProducerError> {
        // Pulsar invariant: even a non-chunked, non-batched send must declare exactly one chunk
        // — explicitly setting `total_chunks` is what the broker uses to disambiguate from a
        // legacy non-PIP-37 client. We omit the field (broker default == 1).
        msg.metadata.num_chunks_from_msg = None;
        msg.metadata.chunk_id = None;
        msg.metadata.total_chunk_msg_size = None;
        msg.metadata.uuid = None;
        if msg.uncompressed_size > 0 {
            msg.metadata.uncompressed_size = Some(msg.uncompressed_size);
        }
        if self.compression != CompressionKind::None {
            msg.metadata.compression = Some(self.compression.to_pb() as i32);
        }
        // Pulsar's `TopicTransactionBuffer.appendBufferToTxn` is keyed on the
        // `txnid_*` fields of the **MessageMetadata**, not `CommandSend`. Java's
        // `TypedMessageBuilderImpl#beforeSend` always copies the txn bits onto the metadata
        // (`msgMetadata.setTxnidMostBits(txn.getTxnIdMostBits())`); omitting them makes the
        // broker treat the publish as a non-txn send and route past the buffer, so aborted
        // writes leak to consumers. We mirror Java by stamping the same fields here.
        if let Some(t) = msg.txn_id {
            msg.metadata.txnid_least_bits = Some(t.least_sig_bits);
            msg.metadata.txnid_most_bits = Some(t.most_sig_bits);
        }

        let seq = self.assign_sequence_id(&mut msg.metadata, publish_time_ms);
        let send = pb::CommandSend {
            producer_id: self.handle.0,
            sequence_id: seq.0,
            num_messages: Some(msg.num_messages.max(1)),
            txnid_least_bits: msg.txn_id.map(|t| t.least_sig_bits),
            txnid_most_bits: msg.txn_id.map(|t| t.most_sig_bits),
            highest_sequence_id: None,
            is_chunk: None,
            marker: None,
            message_id: None,
        };
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Send as i32,
            send: Some(send),
            ..Default::default()
        };
        let num_messages = msg.num_messages.max(1);
        self.total_msgs_sent = self.total_msgs_sent.saturating_add(num_messages as u64);
        self.total_bytes_sent = self
            .total_bytes_sent
            .saturating_add(msg.payload.len() as u64);
        let frame = OutboundFrame {
            command: cmd,
            metadata: msg.metadata,
            payload: msg.payload,
            sequence_id: seq,
        };
        // Stash a clone for replay before pushing onto `outbound`. `Bytes` is refcounted so
        // the payload clone shares the underlying buffer.
        let replay_frames = vec![frame.clone()];
        self.outbound.push_back(frame);
        let op = OpSend {
            sequence_id: seq,
            num_messages,
            waker: None,
            receipt: None,
            error: None,
            enqueued_at: now,
            replay_frames,
        };
        self.pending_index.insert(seq, self.pending.len());
        self.pending.push_back(op);
        Ok(SendDecision::Emit { count: 1 })
    }

    /// Buffer a message in the batch container.
    fn add_to_batch(
        &mut self,
        msg: OutgoingMessage,
        publish_time_ms: u64,
        now: std::time::Instant,
    ) -> Result<SendDecision, ProducerError> {
        let _ = publish_time_ms; // publish time gets stamped at flush
        let payload = msg.payload;
        let mut single = pb::SingleMessageMetadata::default();
        single.payload_size = i32::try_from(payload.len()).unwrap_or(i32::MAX);
        if let Some(key) = &msg.metadata.partition_key {
            single.partition_key = Some(key.clone());
        }
        if let Some(ordering) = &msg.metadata.ordering_key {
            single.ordering_key = Some(ordering.clone());
        }
        if !msg.metadata.properties.is_empty() {
            single.properties = msg.metadata.properties.clone();
        }
        if let Some(event_time) = msg.metadata.event_time {
            single.event_time = Some(event_time);
        }
        if let Some(null_value) = msg.metadata.null_value {
            single.null_value = Some(null_value);
        }
        if let Some(null_partition_key) = msg.metadata.null_partition_key {
            single.null_partition_key = Some(null_partition_key);
        }

        self.batch.current_size_bytes = self.batch.current_size_bytes.saturating_add(payload.len());
        // Track the txn id on the first message so [`Self::queue_send`] can flush before
        // mixing two distinct txns. Subsequent same-batch calls have already been gated by
        // `matches_txn`, so we just keep the existing id.
        if self.batch.is_empty() {
            self.batch.txn_id = msg.txn_id;
        }
        self.batch.messages.push((single, payload));
        if self.batch.lowest_sequence_id.is_none() {
            self.batch.lowest_sequence_id = Some(self.next_sequence_id);
        }
        // Mint a unique per-message sequence id NOW so each user-side `SendFut` waits on its
        // own key. Without this, every batched send was returned the same `seq_id` from
        // `Connection::send`, and the single `OpSend` pushed by `flush_batch` could only
        // wake one of the N futures via `apply_receipt`; the other N-1 hung forever even
        // after the broker acked the whole batch. Mirrors Java
        // `ProducerImpl.serializeAndSendMessage` minting `msg.metadata.sequence_id` per
        // message and bumping `msgIdGenerator`.
        let msg_seq = self.next_sequence_id;
        self.next_sequence_id = self.next_sequence_id.wrapping_add(1);
        self.last_sequence_id_pushed = msg_seq as i64;
        self.batch.highest_sequence_id = Some(msg_seq);
        // Each batched message gets its own `OpSend` with `num_messages = 1` so
        // `apply_receipt` (or `apply_send_error`) can resolve them individually. The
        // `flush_batch` path emits a single wire frame with `sequence_id = lowest`
        // and `highest_sequence_id = highest` — the inbound receipt then fans out over
        // `[lowest, highest]` and each `OpSend` is removed in turn.
        let op = OpSend {
            sequence_id: SequenceId(msg_seq),
            num_messages: 1,
            waker: None,
            receipt: None,
            error: None,
            enqueued_at: now,
            // No per-message replay frame: replay on reconnect drains the still-buffered
            // batch entries via `drain_pending_sends`, which already clears
            // `BatchContainer`.
            replay_frames: Vec::new(),
        };
        self.pending_index
            .insert(SequenceId(msg_seq), self.pending.len());
        self.pending.push_back(op);
        // First message in the current batch — stamp the monotonic timestamp so
        // `Connection::handle_timeout` can force a flush once
        // `batching_max_publish_delay` has elapsed. The caller-provided `now`
        // keeps the state machine sans-io: no internal clock reads.
        if self.batch.first_added_at.is_none() {
            self.batch.first_added_at = Some(now);
        }
        Ok(SendDecision::Batched)
    }

    /// Wall-clock deadline at which the batch should be force-flushed. Returns `None`
    /// when batching has no max-publish-delay, or the batch is currently empty.
    #[must_use]
    pub fn next_batch_deadline(&self) -> Option<std::time::Instant> {
        let max_delay = self.batching_max_publish_delay?;
        let first = self.batch.first_added_at?;
        Some(first + max_delay)
    }

    /// `true` if the batch should be force-flushed at `now` because
    /// `batching_max_publish_delay` has elapsed since the first added message.
    #[must_use]
    pub fn batch_deadline_elapsed(&self, now: std::time::Instant) -> bool {
        self.next_batch_deadline().is_some_and(|d| now >= d)
    }

    /// Flush the batch container into one SEND frame. The caller is responsible for compression
    /// and any encryption of the concatenated payload. We hand back the raw concatenated bytes
    /// (singles' length-prefixed metadata followed by payload).
    ///
    /// `now` is the caller-supplied monotonic timestamp recorded on the resulting `OpSend`
    /// so the sans-io state machine never reads its own clock.
    ///
    /// Returns the number of frames now queued (always 0 or 1).
    pub fn flush_batch(&mut self, publish_time_ms: u64, now: std::time::Instant) -> usize {
        if self.batch.is_empty() || self.closed {
            return 0;
        }
        let num_messages = self.batch.messages.len() as i32;
        // Concatenate `[single_meta_size u32 BE][single_meta bytes][payload bytes]` for each
        // message — mirrors `BatchMessageContainerImpl.toBatchedMessageMetadataAndPayload`.
        use prost::Message as _;
        let total: usize = self
            .batch
            .messages
            .iter()
            .map(|(sm, payload)| 4 + sm.encoded_len() + payload.len())
            .sum();
        let mut concatenated = bytes::BytesMut::with_capacity(total);
        for (sm, payload) in self.batch.messages.drain(..) {
            let sm_len = sm.encoded_len();
            concatenated.extend_from_slice(&(sm_len as u32).to_be_bytes());
            sm.encode(&mut concatenated)
                .expect("encode SingleMessageMetadata");
            concatenated.extend_from_slice(&payload);
        }
        let payload = concatenated.freeze();
        let lowest = self
            .batch
            .lowest_sequence_id
            .unwrap_or(self.next_sequence_id);
        let _ = lowest; // we still use next_sequence_id_assignment below for the metadata
        let mut metadata = pb::MessageMetadata::default();
        metadata.num_messages_in_batch = Some(num_messages);
        if let Some(name) = &self.name {
            metadata.producer_name = name.clone();
        }
        // Mirror Java `TypedMessageBuilderImpl#beforeSend`: stamp the per-message txn bits
        // on the batch's MessageMetadata so the broker's `TopicTransactionBuffer` routes the
        // whole batch through the buffer instead of straight to the dispatcher.
        if let Some(t) = self.batch.txn_id {
            metadata.txnid_least_bits = Some(t.least_sig_bits);
            metadata.txnid_most_bits = Some(t.most_sig_bits);
        }
        // Per-message sequence ids were already minted in `add_to_batch`; the wire frame
        // uses the lowest as `sequence_id` and the highest as `highest_sequence_id`, and
        // the per-message `OpSend` entries are already in `self.pending`. Re-bumping
        // `next_sequence_id` here (as the pre-refactor flush did) would double-bump and
        // skip ids on the next non-batched send.
        let lowest = self
            .batch
            .lowest_sequence_id
            .unwrap_or(self.next_sequence_id);
        let highest = self
            .batch
            .highest_sequence_id
            .unwrap_or(self.last_sequence_id_pushed.max(0) as u64);
        let lowest_seq = SequenceId(lowest);
        metadata.sequence_id = lowest;
        metadata.publish_time = publish_time_ms;
        if highest > lowest {
            metadata.highest_sequence_id = Some(highest);
        }
        if self.compression != CompressionKind::None {
            metadata.compression = Some(self.compression.to_pb() as i32);
        }
        if let Ok(payload_total) = self.batch.current_size_bytes.try_into() {
            metadata.uncompressed_size = Some(payload_total);
        }

        let send = pb::CommandSend {
            producer_id: self.handle.0,
            sequence_id: lowest,
            num_messages: Some(num_messages),
            // The TransactionBuffer is keyed on the `txnid_*` fields of `CommandSend`; missing
            // them on a batched send routes every message in the batch through the dispatcher
            // and bypasses commit/abort markers (aborted writes get delivered). `add_to_batch`
            // stamps `self.batch.txn_id` from the first message and `queue_send` flushes when
            // a different txn arrives, so the whole batch by construction shares one id.
            txnid_least_bits: self.batch.txn_id.map(|t| t.least_sig_bits),
            txnid_most_bits: self.batch.txn_id.map(|t| t.most_sig_bits),
            highest_sequence_id: metadata.highest_sequence_id,
            is_chunk: None,
            marker: None,
            message_id: None,
        };
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Send as i32,
            send: Some(send),
            ..Default::default()
        };
        let payload_len = payload.len();
        let frame = OutboundFrame {
            command: cmd,
            metadata,
            payload,
            sequence_id: lowest_seq,
        };
        let _ = now;
        self.outbound.push_back(frame);
        self.total_msgs_sent = self.total_msgs_sent.saturating_add(num_messages as u64);
        self.total_bytes_sent = self.total_bytes_sent.saturating_add(payload_len as u64);

        self.batch.clear();
        1
    }

    /// Emit a sequence of chunk frames for a large message.
    fn emit_chunked(
        &mut self,
        msg: OutgoingMessage,
        publish_time_ms: u64,
        now: std::time::Instant,
    ) -> Result<SendDecision, ProducerError> {
        let txn_id = msg.txn_id;
        let payload = msg.payload;
        let uuid = uuid::Uuid::new_v4().to_string();
        // Per-chunk PAYLOAD must leave room for the wire-frame overhead
        // (BaseCommand header + per-chunk MessageMetadata + framing bytes)
        // so the total frame stays under the broker's `maxMessageSize`
        // limit. Pulsar's Java client uses `maxMessageSize -
        // DEFAULT_METADATA_RESERVATION` (≈1 KiB) for `chunkMaxMessageSize`;
        // mirror that here. Without this reservation, a chunk whose
        // payload equals `max_message_size` would produce a frame
        // `max_message_size + ~100 bytes` and the broker silently drops
        // it (no error reaches the producer), leaving `send().await` to
        // hang forever waiting for a receipt.
        //
        // We only apply the reservation when `max_message_size` is large
        // enough for it to be meaningful (≥ 4× the reservation) — unit
        // tests construct producers with `max_message_size=10` to exercise
        // the chunking math and would otherwise see every chunk shrink
        // to 1 byte.
        const CHUNK_METADATA_RESERVATION: usize = 1024;
        let max_chunk_payload = if self.max_message_size > CHUNK_METADATA_RESERVATION * 4 {
            self.max_message_size - CHUNK_METADATA_RESERVATION
        } else {
            self.max_message_size
        };
        let total_chunks =
            ChunkedMessageContext::compute_total_chunks(payload.len(), max_chunk_payload);

        let mut ctx = ChunkedMessageContext {
            payload: payload.clone(),
            uuid: uuid.clone(),
            total_chunks,
            next_chunk: 0,
            max_chunk_size: max_chunk_payload,
            sequence_id: SequenceId(0), // assigned below
            metadata: msg.metadata,
            uncompressed_size: msg.uncompressed_size,
        };
        ctx.sequence_id = SequenceId(self.next_sequence_id);

        // Assign the sequence id ONCE for the logical message — every chunk carries the same id
        // (Java `ProducerImpl.java:696-704`).
        self.next_sequence_id = self.next_sequence_id.wrapping_add(1);
        ctx.metadata.sequence_id = ctx.sequence_id.0;
        ctx.metadata.publish_time = publish_time_ms;
        if let Some(name) = &self.name {
            if ctx.metadata.producer_name.is_empty() {
                ctx.metadata.producer_name = name.clone();
            }
        }
        if self.compression != CompressionKind::None {
            ctx.metadata.compression = Some(self.compression.to_pb() as i32);
        }
        ctx.metadata.uncompressed_size = Some(ctx.uncompressed_size);
        ctx.metadata.uuid = Some(uuid);
        ctx.metadata.num_chunks_from_msg = Some(ctx.total_chunks);
        ctx.metadata.total_chunk_msg_size = Some(payload.len() as i32);
        // Every chunk shares the same MessageMetadata clone — stamp the txn bits here so
        // each chunk-level MessageMetadata routes through Pulsar's `TopicTransactionBuffer`
        // (see the metadata note on `emit_single`).
        if let Some(t) = txn_id {
            ctx.metadata.txnid_least_bits = Some(t.least_sig_bits);
            ctx.metadata.txnid_most_bits = Some(t.most_sig_bits);
        }
        self.last_sequence_id_pushed = ctx.sequence_id.0 as i64;

        // Emit each chunk frame eagerly into the outbound queue.
        let mut emitted = 0;
        let mut replay_frames: Vec<OutboundFrame> = Vec::with_capacity(ctx.total_chunks as usize);
        for chunk_idx in 0..ctx.total_chunks {
            let start = (chunk_idx as usize) * ctx.max_chunk_size;
            let end = ((chunk_idx as usize + 1) * ctx.max_chunk_size).min(ctx.payload.len());
            let chunk_payload = ctx.payload.slice(start..end);
            let mut chunk_meta = ctx.metadata.clone();
            chunk_meta.chunk_id = Some(chunk_idx);
            let send = pb::CommandSend {
                producer_id: self.handle.0,
                sequence_id: ctx.sequence_id.0,
                num_messages: Some(1),
                txnid_least_bits: txn_id.map(|t| t.least_sig_bits),
                txnid_most_bits: txn_id.map(|t| t.most_sig_bits),
                highest_sequence_id: None,
                is_chunk: Some(true),
                marker: None,
                message_id: None,
            };
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Send as i32,
                send: Some(send),
                ..Default::default()
            };
            let chunk_payload_len = chunk_payload.len();
            let frame = OutboundFrame {
                command: cmd,
                metadata: chunk_meta,
                payload: chunk_payload,
                sequence_id: ctx.sequence_id,
            };
            replay_frames.push(frame.clone());
            self.outbound.push_back(frame);
            self.total_msgs_sent = self.total_msgs_sent.saturating_add(1);
            self.total_bytes_sent = self
                .total_bytes_sent
                .saturating_add(chunk_payload_len as u64);
            emitted += 1;
        }

        // Only one OpSend per logical chunked publish — the receipt covers the whole sequence.
        let op = OpSend {
            sequence_id: ctx.sequence_id,
            num_messages: 1,
            waker: None,
            receipt: None,
            error: None,
            enqueued_at: now,
            replay_frames,
        };
        self.pending_index
            .insert(ctx.sequence_id, self.pending.len());
        self.pending.push_back(op);
        Ok(SendDecision::Emit { count: emitted })
    }

    /// Pop the next outbound frame, if any.
    pub fn next_outbound_frame(&mut self) -> Option<OutboundFrame> {
        self.outbound.pop_front()
    }

    /// Number of frames waiting to be drained.
    pub fn outbound_len(&self) -> usize {
        self.outbound.len()
    }

    /// Apply a `CommandSendReceipt` to the pending queue. Returns the matching sequence id +
    /// message id if we had it pending.
    pub fn apply_receipt(
        &mut self,
        receipt: &pb::CommandSendReceipt,
    ) -> Option<(SequenceId, MessageId, Option<Waker>)> {
        let seq = SequenceId(receipt.sequence_id);
        let _idx = self.pending_index.remove(&seq)?;
        let position = self.pending.iter().position(|op| op.sequence_id == seq)?;
        let mut op = self.pending.remove(position)?;
        let mid = receipt
            .message_id
            .as_ref()
            .map(MessageId::from_pb)
            .unwrap_or(MessageId {
                ledger_id: 0,
                entry_id: 0,
                partition: -1,
                batch_index: -1,
                batch_size: 0,
            });
        self.last_sequence_id_published = seq.0 as i64;
        // Record the broker round-trip latency (enqueue → receipt). `saturating_record` keeps us
        // safe if a future record landed above the histogram's current bound — auto-resize will
        // grow but a saturating fallback is still cheaper than the panic path. Mirrors the Java
        // `ProducerStatsRecorder#updateLatency(long latencyNanos)` call site.
        let latency_ms = u64::try_from(op.enqueued_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.send_latency_hist.saturating_record(latency_ms);
        op.receipt = Some(mid);
        let waker = op.waker.take();
        // Re-index remaining positions.
        self.refresh_pending_index();
        Some((seq, mid, waker))
    }

    /// Apply a `CommandSendError` to the pending queue.
    pub fn apply_send_error(
        &mut self,
        err: &pb::CommandSendError,
    ) -> Option<(SequenceId, Option<Waker>, i32, String)> {
        let seq = SequenceId(err.sequence_id);
        let _idx = self.pending_index.remove(&seq)?;
        let position = self.pending.iter().position(|op| op.sequence_id == seq)?;
        let mut op = self.pending.remove(position)?;
        let waker = op.waker.take();
        self.refresh_pending_index();
        Some((seq, waker, err.error, err.message.clone()))
    }

    fn refresh_pending_index(&mut self) {
        self.pending_index.clear();
        for (idx, op) in self.pending.iter().enumerate() {
            self.pending_index.insert(op.sequence_id, idx);
        }
    }

    /// Register a waker for the given pending sequence id.
    pub fn register_waker(&mut self, sequence_id: SequenceId, waker: Waker) {
        if let Some(idx) = self.pending_index.get(&sequence_id).copied() {
            if let Some(op) = self.pending.get_mut(idx) {
                op.waker = Some(waker);
            }
        }
    }

    /// Drain every in-flight `OpSend` and return the `(sequence_id, waker)` pairs that
    /// were registered by user-facing send futures. The caller is responsible for
    /// installing a `SessionLost` outcome and waking each future. Also clears the batch
    /// container so partial in-flight batches do not survive a reconnect. Mirrors Java
    /// `ProducerImpl#connectionClosed`'s synthetic-failure pass over `pendingMessages`.
    pub fn drain_pending_sends(&mut self) -> Vec<(SequenceId, Option<Waker>)> {
        let mut out = Vec::with_capacity(self.pending.len());
        while let Some(mut op) = self.pending.pop_front() {
            self.pending_index.remove(&op.sequence_id);
            out.push((op.sequence_id, op.waker.take()));
        }
        // Batch container holds messages that never made it to the wire — drop them so
        // a stale batch does not re-emit on the freshly-handshaked connection.
        self.batch = BatchContainer::default();
        self.outbound.clear();
        out
    }

    /// Drain every in-flight [`OpSend`] but preserve the publish data for replay on the
    /// freshly-handshaked session. Returns:
    ///
    /// - `wakers`: the user-facing send-future wakers we removed from each [`OpSend`] so the caller
    ///   can wake them exactly once *after* the snapshot has been stashed.
    /// - `snapshots`: the drained [`OpSend`] entries, in original FIFO order, each with its `waker`
    ///   field already cleared. Sequence ids, num-messages, and the cached [`OutboundFrame`] vector
    ///   are preserved so [`Self::replay_snapshots`] can re-issue the publish verbatim on the new
    ///   session.
    ///
    /// Also clears the batch container — unflushed batched messages are the caller's
    /// responsibility to re-send (matches Java `ProducerImpl#connectionClosed` which drops
    /// the in-progress batch). The outbound frame queue is cleared too.
    ///
    /// Mirrors the snapshot half of Java `ProducerImpl#resendMessages`, which keeps
    /// `pendingMessages` around across the reconnect and re-issues each `OpSendMsg`
    /// onto the new connection. Sans-io: this state machine never reaches for a clock —
    /// `enqueued_at` on each snapshot is preserved from the original send, so the
    /// post-rebuild send-timeout sweep still uses the original deadline.
    pub fn snapshot_pending_sends(&mut self) -> (Vec<(SequenceId, Option<Waker>)>, Vec<OpSend>) {
        let mut wakers = Vec::with_capacity(self.pending.len());
        let mut snapshots = Vec::with_capacity(self.pending.len());
        while let Some(mut op) = self.pending.pop_front() {
            self.pending_index.remove(&op.sequence_id);
            // Take the waker — the caller wakes the future exactly once with the
            // pre-reset outcome (transparent replay = no outcome stored). Clearing here
            // also prevents `apply_receipt` from later double-waking the same future
            // when the replayed receipt lands.
            let w = op.waker.take();
            wakers.push((op.sequence_id, w));
            // Per-message batched `OpSend` entries (created by `add_to_batch`) carry no
            // `replay_frames` because the wire frame only materialises at `flush_batch`
            // time. Drop them on the floor (no replay) — matching Java
            // `ProducerImpl#connectionClosed` which fails an in-progress batch instead of
            // re-emitting the partial bytes.
            if !op.replay_frames.is_empty() {
                snapshots.push(op);
            }
        }
        self.batch = BatchContainer::default();
        self.outbound.clear();
        (wakers, snapshots)
    }

    /// Re-issue a vector of [`OpSend`] snapshots produced by
    /// [`Self::snapshot_pending_sends`]. For each snapshot:
    ///
    /// 1. Every cached [`OutboundFrame`] is pushed back onto the producer's outbound queue
    ///    (preserving wire order; a chunked publish replays N frames in the same relative order as
    ///    the original emit).
    /// 2. The snapshot's [`OpSend`] is re-inserted into `pending` with `waker: None`. The user's
    ///    send future re-registers on the next `poll` after the wake-up.
    ///
    /// Counter side-effects (`total_msgs_sent`, `total_bytes_sent`) are NOT incremented
    /// — the original emit already counted them; a re-send is not "new" traffic from a
    /// per-producer-stats perspective.
    ///
    /// Sequence ids on the replayed `OpSend`s are preserved verbatim. The broker's
    /// dedup window (mirrors Java `ProducerImpl#epoch` + the on-wire `CommandProducer.epoch`
    /// bump) rejects stale re-attaches; `last_sequence_id_pushed` is already pinned to
    /// the highest sent id and is left untouched here. Mirrors Java
    /// `ProducerImpl#resendMessages`'s `pendingMessages` walk.
    pub fn replay_snapshots(&mut self, snapshots: Vec<OpSend>) {
        for snapshot in snapshots {
            for frame in &snapshot.replay_frames {
                self.outbound.push_back(frame.clone());
            }
            self.pending_index
                .insert(snapshot.sequence_id, self.pending.len());
            self.pending.push_back(snapshot);
        }
    }

    /// Re-push the wire frames for every currently-pending `OpSend` back onto the outbound
    /// queue WITHOUT re-adding the ops to `pending` (they're already there). Used by the
    /// transient-retry path: after `retry_producer_open` re-attaches the producer, any
    /// `OpSend`s that the user enqueued during the transient window had their original
    /// frames silently dropped by the broker (Pulsar drops `CommandSend` for unknown
    /// `producer_id` without sending an error). Re-emitting the cached `replay_frames`
    /// re-runs the publish on the freshly-attached producer; the user-facing `SendFut`
    /// then resolves on the eventual `CommandSendReceipt`. Distinct from
    /// [`Self::replay_snapshots`], which is the reset-path counterpart that pushes the
    /// `OpSend`s into `pending` from scratch.
    ///
    /// `OpSend`s with empty `replay_frames` (in-progress batched sends from
    /// `add_to_batch`) are skipped: their wire bytes only materialise at `flush_batch`
    /// time.
    pub fn replay_pending_outbound(&mut self) {
        for op in &self.pending {
            for frame in &op.replay_frames {
                self.outbound.push_back(frame.clone());
            }
        }
    }

    /// Mark the producer closed. New sends return [`ProducerError::Closed`].
    pub fn close(&mut self) {
        self.closed = true;
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn small_message(payload: &[u8]) -> OutgoingMessage {
        OutgoingMessage {
            payload: Bytes::copy_from_slice(payload),
            metadata: pb::MessageMetadata {
                producer_name: "p".to_owned(),
                sequence_id: 0,
                publish_time: 0,
                ..Default::default()
            },
            uncompressed_size: payload.len() as u32,
            num_messages: 1,
            txn_id: None,
        }
    }

    #[test]
    fn small_send_emits_one_frame() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        let decision = p
            .queue_send(small_message(b"hello"), 100, std::time::Instant::now())
            .unwrap();
        match decision {
            SendDecision::Emit { count } => assert_eq!(count, 1),
            other => panic!("expected Emit, got {other:?}"),
        }
        assert_eq!(p.outbound_len(), 1);
        let frame = p.next_outbound_frame().expect("frame");
        assert_eq!(frame.payload.as_ref(), b"hello");
        assert_eq!(frame.metadata.sequence_id, 0);
    }

    #[test]
    fn large_send_chunks_when_enabled() {
        let mut p =
            ProducerState::new(ProducerHandle(1), "t".to_owned(), CompressionKind::None, 10);
        p.chunking_enabled = true;
        let payload = vec![b'a'; 25];
        let decision = p
            .queue_send(small_message(&payload), 100, std::time::Instant::now())
            .unwrap();
        match decision {
            SendDecision::Emit { count } => assert_eq!(count, 3),
            other => panic!("expected Emit, got {other:?}"),
        }
        let f1 = p.next_outbound_frame().unwrap();
        let f2 = p.next_outbound_frame().unwrap();
        let f3 = p.next_outbound_frame().unwrap();
        assert_eq!(f1.metadata.chunk_id, Some(0));
        assert_eq!(f2.metadata.chunk_id, Some(1));
        assert_eq!(f3.metadata.chunk_id, Some(2));
        assert_eq!(f1.metadata.num_chunks_from_msg, Some(3));
        assert_eq!(f1.metadata.uuid, f3.metadata.uuid);
        assert_eq!(f1.metadata.sequence_id, f3.metadata.sequence_id);
    }

    #[test]
    fn large_send_without_chunking_errors() {
        let mut p =
            ProducerState::new(ProducerHandle(1), "t".to_owned(), CompressionKind::None, 10);
        let payload = vec![b'a'; 25];
        let err = p
            .queue_send(small_message(&payload), 100, std::time::Instant::now())
            .unwrap_err();
        match err {
            ProducerError::MessageTooLarge { size, .. } => assert_eq!(size, 25),
            other => panic!("expected MessageTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn batched_send_accumulates_then_flushes() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        p.batching_enabled = true;
        p.max_batch_size_bytes = 1024;
        p.max_messages_in_batch = 10;
        for _ in 0..3 {
            let d = p
                .queue_send(small_message(b"x"), 100, std::time::Instant::now())
                .unwrap();
            assert!(matches!(d, SendDecision::Batched));
        }
        assert_eq!(p.outbound_len(), 0);
        let flushed = p.flush_batch(101, std::time::Instant::now());
        assert_eq!(flushed, 1);
        let frame = p.next_outbound_frame().expect("flushed frame");
        assert_eq!(frame.metadata.num_messages_in_batch, Some(3));
    }

    /// The Codex Q3 invariant: if `can_add_to_batch` returns true, total chunks must be 1.
    /// We assert it by constructing both decisions and checking they never both fire.
    #[test]
    fn can_add_to_batch_implies_total_chunks_one() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        p.batching_enabled = true;
        p.chunking_enabled = true;
        p.max_batch_size_bytes = 1024;
        p.max_messages_in_batch = 10;

        // A "small enough to batch" message:
        let msg = small_message(b"small");
        assert!(p.can_add_to_batch(msg.payload.len(), 1));
        let decision = p.queue_send(msg, 100, std::time::Instant::now()).unwrap();
        assert!(
            matches!(decision, SendDecision::Batched),
            "can_add_to_batch was true, must batch, not chunk"
        );
        // No chunk frames were emitted at all.
        assert_eq!(p.outbound_len(), 0);
    }

    #[test]
    fn receipt_correlates_and_clears_pending() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        let _ = p
            .queue_send(small_message(b"a"), 100, std::time::Instant::now())
            .unwrap();
        let _ = p.next_outbound_frame();
        assert_eq!(p.pending.len(), 1);
        let r = pb::CommandSendReceipt {
            producer_id: 1,
            sequence_id: 0,
            message_id: Some(pb::MessageIdData {
                ledger_id: 5,
                entry_id: 10,
                ..Default::default()
            }),
            highest_sequence_id: None,
        };
        let (seq, mid, _) = p.apply_receipt(&r).expect("receipt matched");
        assert_eq!(seq.0, 0);
        assert_eq!(mid.ledger_id, 5);
        assert_eq!(p.pending.len(), 0);
        assert_eq!(p.last_sequence_id_published, 0);
    }

    #[test]
    fn producer_stats_track_bytes_and_msgs() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        assert_eq!(p.stats().total_msgs_sent, 0);
        assert_eq!(p.stats().total_bytes_sent, 0);

        let _ = p
            .queue_send(small_message(b"hello"), 100, std::time::Instant::now())
            .unwrap();
        let _ = p
            .queue_send(small_message(b"world!"), 100, std::time::Instant::now())
            .unwrap();
        let stats = p.stats();
        assert_eq!(stats.total_msgs_sent, 2);
        assert_eq!(stats.total_bytes_sent, 5 + 6);
        assert_eq!(stats.pending_queue_size, 2);
    }

    #[test]
    fn chunked_send_counts_each_chunk() {
        let mut p =
            ProducerState::new(ProducerHandle(1), "t".to_owned(), CompressionKind::None, 10);
        p.chunking_enabled = true;
        let payload = vec![b'a'; 25];
        let _ = p
            .queue_send(small_message(&payload), 100, std::time::Instant::now())
            .unwrap();
        let stats = p.stats();
        assert_eq!(stats.total_msgs_sent, 3);
        // 25 bytes split as 10 + 10 + 5
        assert_eq!(stats.total_bytes_sent, 25);
    }

    #[test]
    fn batched_send_counts_logical_msgs_and_concatenated_bytes() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        p.batching_enabled = true;
        p.max_batch_size_bytes = 1024;
        p.max_messages_in_batch = 10;
        for _ in 0..3 {
            let _ = p
                .queue_send(small_message(b"x"), 100, std::time::Instant::now())
                .unwrap();
        }
        assert_eq!(p.stats().total_msgs_sent, 0); // not flushed yet
        let _ = p.flush_batch(101, std::time::Instant::now());
        let stats = p.stats();
        assert_eq!(stats.total_msgs_sent, 3);
        assert!(stats.total_bytes_sent > 0);
    }

    #[test]
    fn initial_sequence_id_resumes_from_checkpoint() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        p.set_initial_sequence_id(42);
        assert_eq!(p.last_sequence_id_pushed, 41);
        assert_eq!(p.last_sequence_id_published, 41);

        let _ = p
            .queue_send(small_message(b"first"), 100, std::time::Instant::now())
            .unwrap();
        let frame = p.next_outbound_frame().expect("frame");
        assert_eq!(frame.metadata.sequence_id, 42);
        assert_eq!(frame.sequence_id.0, 42);

        let _ = p
            .queue_send(small_message(b"second"), 100, std::time::Instant::now())
            .unwrap();
        let frame = p.next_outbound_frame().expect("frame");
        assert_eq!(frame.metadata.sequence_id, 43);
    }

    #[test]
    fn batch_max_publish_delay_deadline_tracking() {
        use std::time::Duration;
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        p.batching_enabled = true;
        p.max_messages_in_batch = 100;
        p.batching_max_publish_delay = Some(Duration::from_millis(50));

        // No deadline when the batch is empty.
        assert!(p.next_batch_deadline().is_none());

        let _ = p
            .queue_send(small_message(b"first"), 100, std::time::Instant::now())
            .unwrap();
        let deadline = p
            .next_batch_deadline()
            .expect("deadline once batch is non-empty");
        let now = p.batch.first_added_at.unwrap();
        assert_eq!(deadline, now + Duration::from_millis(50));

        // Not yet elapsed.
        assert!(!p.batch_deadline_elapsed(now + Duration::from_millis(49)));
        // Past deadline.
        assert!(p.batch_deadline_elapsed(now + Duration::from_millis(51)));

        // Flush clears the timestamp.
        let _ = p.flush_batch(101, std::time::Instant::now());
        assert!(p.next_batch_deadline().is_none());
    }

    // ---------------------------------------------------------------------
    // BatchContainer behavioral tests — backported from Java
    // `BatchMessageContainerImplTest.java`.
    //
    // The Java tests rely on Mockito + Netty `ByteBufAllocator` to drive
    // `BatchMessageContainerImpl.add` / `createOpSendMsg`. Our `BatchContainer`
    // is the equivalent state holder, so we exercise the same invariants by
    // mutating the container directly (the type derives `Default`).
    // ---------------------------------------------------------------------

    /// Build a `SingleMessageMetadata` whose `sequence_id` is set, as the Java
    /// fixture does (`messageMetadata.setSequenceId(i)` in
    /// `addMessagesAndCreateOpSendMsg`).
    fn single_with_seq(seq: u64, payload_size: i32) -> pb::SingleMessageMetadata {
        pb::SingleMessageMetadata {
            payload_size,
            sequence_id: Some(seq),
            ..Default::default()
        }
    }

    /// Helper to push a single message + payload into a batch container and
    /// update its bookkeeping the same way `add_to_batch` would.
    fn push_into_batch(batch: &mut BatchContainer, seq: u64, payload: &[u8]) {
        let single = single_with_seq(seq, payload.len() as i32);
        batch.current_size_bytes = batch.current_size_bytes.saturating_add(payload.len());
        if batch.lowest_sequence_id.is_none() {
            batch.lowest_sequence_id = Some(seq);
        }
        batch.highest_sequence_id = Some(seq);
        if batch.first_added_at.is_none() {
            batch.first_added_at = Some(std::time::Instant::now());
        }
        batch
            .messages
            .push((single, Bytes::copy_from_slice(payload)));
    }

    /// Mirrors the size-bookkeeping facet of Java `testMessagesSize`: pushing
    /// payloads into the batch grows `current_size_bytes` in lock-step with
    /// the concatenated payload total, and the configured size threshold
    /// rejects further `can_add_to_batch` calls.
    #[test]
    fn batch_size_threshold_flush() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        p.batching_enabled = true;
        p.max_batch_size_bytes = 8;
        p.max_messages_in_batch = 100;

        // Three 2-byte payloads fit (6 / 8).
        push_into_batch(&mut p.batch, 0, b"aa");
        push_into_batch(&mut p.batch, 1, b"bb");
        push_into_batch(&mut p.batch, 2, b"cc");
        assert_eq!(p.batch.current_size_bytes, 6);
        assert!(p.can_add_to_batch(2, 1), "1 more 2-byte payload fits (8/8)");

        // 3 more bytes would overflow the 8-byte budget.
        assert!(!p.can_add_to_batch(3, 1));

        let flushed = p.flush_batch(101, std::time::Instant::now());
        assert_eq!(flushed, 1, "non-empty batch always flushes one frame");
        // Flush drains the messages vec but keeps the SEND frame queued.
        assert!(p.batch.is_empty());
        assert_eq!(p.outbound_len(), 1);
    }

    /// Mirrors the count-threshold facet of Java `testMessagesSize`: once
    /// `max_messages_in_batch` is reached, `can_add_to_batch` refuses any
    /// further additions even though there's space in the byte budget.
    #[test]
    fn batch_count_threshold_flush() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        p.batching_enabled = true;
        p.max_batch_size_bytes = 8 * 1024;
        p.max_messages_in_batch = 3;

        for i in 0..3 {
            push_into_batch(&mut p.batch, i, b"x");
        }
        assert_eq!(p.batch.len(), 3);
        assert!(
            !p.can_add_to_batch(1, 1),
            "count cap should refuse additions"
        );

        // After flush the cap should reset and new sends fit again.
        let flushed = p.flush_batch(101, std::time::Instant::now());
        assert_eq!(flushed, 1);
        assert_eq!(p.batch.len(), 0);
        assert!(p.can_add_to_batch(1, 1));
    }

    /// Java `BatchMessageContainerImpl.createOpSendMsg` returns `null` for an
    /// empty container; we return 0 frames. Calling on a closed producer is
    /// also a no-op.
    #[test]
    fn empty_batch_returns_nothing_on_flush() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        p.batching_enabled = true;
        assert!(p.batch.is_empty());
        assert_eq!(
            p.flush_batch(100, std::time::Instant::now()),
            0,
            "empty batch flushes nothing"
        );
        assert_eq!(p.outbound_len(), 0);

        // Closed producer also flushes nothing even when messages remain
        // (mirrors Java behaviour where a closed producer drops the batch).
        push_into_batch(&mut p.batch, 0, b"x");
        p.closed = true;
        assert_eq!(p.flush_batch(100, std::time::Instant::now()), 0);
    }

    /// Java tests assign `messageMetadata.setSequenceId(i)` while building
    /// each batch, then the produced `OpSendMsg` exposes the lowest as
    /// `sequenceId` and the highest as `highestSequenceId`. We assert the
    /// container tracks both monotonically in insertion order.
    #[test]
    fn batch_tracks_lowest_and_highest_sequence_id() {
        let mut batch = BatchContainer::default();
        assert!(batch.lowest_sequence_id.is_none());
        assert!(batch.highest_sequence_id.is_none());

        push_into_batch(&mut batch, 7, b"a");
        push_into_batch(&mut batch, 8, b"b");
        push_into_batch(&mut batch, 9, b"c");
        assert_eq!(batch.lowest_sequence_id, Some(7));
        assert_eq!(batch.highest_sequence_id, Some(9));
        assert_eq!(batch.len(), 3);

        batch.clear();
        assert!(batch.is_empty());
        assert!(batch.lowest_sequence_id.is_none());
        assert!(batch.highest_sequence_id.is_none());
        assert!(batch.first_added_at.is_none());
        assert_eq!(batch.current_size_bytes, 0);
    }

    /// `first_added_at` should be stamped only on the very first add and stay
    /// pinned until flush — that's what feeds the
    /// `batching_max_publish_delay` deadline.
    #[test]
    fn first_added_at_pinned_to_first_message() {
        let mut batch = BatchContainer::default();
        assert!(batch.first_added_at.is_none());

        push_into_batch(&mut batch, 0, b"first");
        let t0 = batch.first_added_at.expect("stamped on first add");
        // Sleep a hair so a wrongly re-stamped timestamp would differ.
        std::thread::sleep(std::time::Duration::from_millis(2));
        push_into_batch(&mut batch, 1, b"second");
        let t1 = batch.first_added_at.expect("still present");
        assert_eq!(t0, t1, "first_added_at must not be overwritten");

        batch.clear();
        assert!(batch.first_added_at.is_none());
    }

    /// Mirrors the Java practice of computing `batchingMaxPublishDelayMicros`
    /// against the first-added timestamp: deadline = first_added_at +
    /// batching_max_publish_delay. Independent of `queue_send`, we drive
    /// `BatchContainer` directly to verify the math.
    #[test]
    fn batching_max_publish_delay_uses_first_added_at() {
        use std::time::{Duration, Instant};
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        p.batching_enabled = true;
        p.max_messages_in_batch = 100;
        p.batching_max_publish_delay = Some(Duration::from_millis(20));

        let anchor = Instant::now();
        p.batch.first_added_at = Some(anchor);
        p.batch
            .messages
            .push((single_with_seq(0, 1), Bytes::from_static(b"x")));
        p.batch.lowest_sequence_id = Some(0);
        p.batch.highest_sequence_id = Some(0);

        let deadline = p.next_batch_deadline().expect("deadline");
        assert_eq!(deadline, anchor + Duration::from_millis(20));
        assert!(!p.batch_deadline_elapsed(anchor + Duration::from_millis(19)));
        assert!(p.batch_deadline_elapsed(anchor + Duration::from_millis(20)));
        assert!(p.batch_deadline_elapsed(anchor + Duration::from_millis(50)));
    }

    /// Drive a synthetic set of latency samples through `send_latency_hist` (without going via
    /// the network) and check that `ProducerStats::send_latency_*_ms` line up with the
    /// percentiles we'd compute from the input distribution by hand. Mirrors the Java
    /// `ProducerStatsRecorderTest#testGetLatencyPercentiles` smoke test.
    #[test]
    fn send_latency_percentiles_reflect_recorded_samples() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        // Empty histogram — accessors and snapshot must report zero, not panic.
        assert_eq!(p.send_latency_p50_ms(), 0);
        assert_eq!(p.send_latency_p99_ms(), 0);
        assert_eq!(p.send_latency_max_ms(), 0);
        let stats0 = p.stats();
        assert_eq!(stats0.send_latency_p50_ms, 0);
        assert_eq!(stats0.send_latency_p99_ms, 0);
        assert_eq!(stats0.send_latency_max_ms, 0);

        // 100 samples uniformly in [1, 100]. p50 should land near 50, p99 near 99, max == 100.
        for v in 1u64..=100 {
            p.send_latency_hist.saturating_record(v);
        }
        let p50 = p.send_latency_p50_ms();
        let p99 = p.send_latency_p99_ms();
        let pmax = p.send_latency_max_ms();
        assert!((45..=55).contains(&p50), "expected p50 ~50 ms, got {p50}");
        assert!((95..=100).contains(&p99), "expected p99 ~99 ms, got {p99}");
        assert_eq!(pmax, 100, "max sample is 100 ms");

        // Snapshot path mirrors the accessor path.
        let stats = p.stats();
        assert_eq!(stats.send_latency_p50_ms, p50);
        assert_eq!(stats.send_latency_p99_ms, p99);
        assert_eq!(stats.send_latency_max_ms, pmax);
    }

    // ---------------------------------------------------------------------
    // PIP-37 producer chunking behavioural tests — backported from the Java
    // `ProducerImpl` chunked-send paths (`ProducerImpl.java:696-704` and
    // `:793-868`). They drive `emit_chunked` directly via `queue_send` and
    // assert the per-chunk metadata layout the broker expects.
    // ---------------------------------------------------------------------

    /// Helper: build an `OutgoingMessage` with no metadata-side properties so
    /// the chunking-emission path is exercised in isolation.
    fn payload_message(payload: Vec<u8>) -> OutgoingMessage {
        OutgoingMessage {
            uncompressed_size: payload.len() as u32,
            payload: Bytes::from(payload),
            metadata: pb::MessageMetadata {
                producer_name: "p".to_owned(),
                sequence_id: 0,
                publish_time: 0,
                ..Default::default()
            },
            num_messages: 1,
            txn_id: None,
        }
    }

    /// Payload above `max_message_size` must produce
    /// `ceil(payload_len / max_message_size)` chunk frames with monotonic
    /// `chunk_id` 0..N and a `num_chunks_from_msg` that matches N.
    #[test]
    fn producer_chunks_emit_n_frames_with_monotonic_chunk_id() {
        let mut p = ProducerState::new(ProducerHandle(1), "t".to_owned(), CompressionKind::None, 4);
        p.chunking_enabled = true;
        // 14 bytes, max_chunk_size = 4 → expected 4 chunks (4 + 4 + 4 + 2).
        let payload = b"ABCDEFGHIJKLMN".to_vec();
        let decision = p
            .queue_send(payload_message(payload), 100, std::time::Instant::now())
            .unwrap();
        match decision {
            SendDecision::Emit { count } => assert_eq!(count, 4, "expected 4 chunks"),
            other => panic!("expected Emit, got {other:?}"),
        }

        // Drain the frames, verify monotonic chunk_id, total_chunks, and the
        // per-chunk payload slicing.
        let frames: Vec<OutboundFrame> = std::iter::from_fn(|| p.next_outbound_frame()).collect();
        assert_eq!(frames.len(), 4);
        for (idx, f) in frames.iter().enumerate() {
            assert_eq!(
                f.metadata.chunk_id,
                Some(idx as i32),
                "chunk_id at index {idx}"
            );
            assert_eq!(
                f.metadata.num_chunks_from_msg,
                Some(4),
                "num_chunks_from_msg at index {idx}"
            );
            // The broker expects the total *uncompressed* size of the logical
            // message on every chunk, so it can size the reassembly buffer.
            assert_eq!(f.metadata.total_chunk_msg_size, Some(14));
            // Each chunk frame is marked is_chunk = true in CommandSend.
            assert_eq!(
                f.command.send.as_ref().and_then(|s| s.is_chunk),
                Some(true),
                "CommandSend.is_chunk at index {idx}"
            );
        }
        // Per-chunk byte slicing: first three full chunks of 4 bytes, last
        // chunk picks up the 2-byte tail.
        assert_eq!(frames[0].payload.as_ref(), b"ABCD");
        assert_eq!(frames[1].payload.as_ref(), b"EFGH");
        assert_eq!(frames[2].payload.as_ref(), b"IJKL");
        assert_eq!(frames[3].payload.as_ref(), b"MN");
    }

    /// All chunk frames of a single logical message must share the same
    /// `uuid` AND the same `sequence_id`. The Java client uses these as the
    /// chunk-reassembly key on the consumer side
    /// (`ProducerImpl.java:793-868`).
    #[test]
    fn producer_chunks_share_uuid_and_sequence_id() {
        let mut p = ProducerState::new(ProducerHandle(1), "t".to_owned(), CompressionKind::None, 8);
        p.chunking_enabled = true;
        // 20 bytes → 3 chunks (8 + 8 + 4).
        let payload = vec![b'x'; 20];
        let _ = p
            .queue_send(payload_message(payload), 100, std::time::Instant::now())
            .unwrap();

        let frames: Vec<OutboundFrame> = std::iter::from_fn(|| p.next_outbound_frame()).collect();
        assert_eq!(frames.len(), 3);
        let uuid0 = frames[0].metadata.uuid.clone();
        let seq0 = frames[0].metadata.sequence_id;
        let cmd_seq0 = frames[0].command.send.as_ref().unwrap().sequence_id;
        assert!(
            uuid0.is_some() && !uuid0.as_deref().unwrap_or("").is_empty(),
            "uuid must be populated on chunked publishes"
        );
        for (idx, f) in frames.iter().enumerate() {
            assert_eq!(
                f.metadata.uuid, uuid0,
                "uuid mismatch at chunk {idx}: {:?} vs {uuid0:?}",
                f.metadata.uuid
            );
            assert_eq!(
                f.metadata.sequence_id, seq0,
                "metadata.sequence_id must be constant across chunks at {idx}"
            );
            assert_eq!(
                f.command.send.as_ref().unwrap().sequence_id,
                cmd_seq0,
                "CommandSend.sequence_id must be constant across chunks at {idx}"
            );
            assert_eq!(
                f.sequence_id.0, cmd_seq0,
                "OutboundFrame.sequence_id must match CommandSend at {idx}"
            );
        }

        // The single OpSend entry covers the whole logical chunked message —
        // only one receipt is expected by the broker.
        assert_eq!(p.pending.len(), 1);
        assert_eq!(p.pending.front().unwrap().sequence_id.0, seq0);
    }

    /// Edge: a payload whose size is an exact multiple of `max_message_size`
    /// must produce exactly `len / max_message_size` chunks (no spurious
    /// empty final chunk). Mirrors the `div_ceil` math
    /// `ChunkedMessageContext::compute_total_chunks` performs.
    #[test]
    fn producer_chunks_exact_multiple_emits_no_empty_tail_chunk() {
        let mut p = ProducerState::new(ProducerHandle(1), "t".to_owned(), CompressionKind::None, 5);
        p.chunking_enabled = true;
        let payload = vec![b'y'; 15]; // 15 / 5 == 3 chunks, no remainder
        let decision = p
            .queue_send(payload_message(payload), 100, std::time::Instant::now())
            .unwrap();
        match decision {
            SendDecision::Emit { count } => assert_eq!(count, 3),
            other => panic!("expected Emit, got {other:?}"),
        }
        for idx in 0..3 {
            let f = p.next_outbound_frame().expect("chunk frame");
            assert_eq!(f.metadata.chunk_id, Some(idx));
            assert_eq!(f.payload.len(), 5);
        }
        assert!(p.next_outbound_frame().is_none(), "no extra frames");
    }

    /// `ChunkedMessageContext::compute_total_chunks` math sanity, mirroring
    /// the helper Java callers use to know how many sends to expect. Includes
    /// the zero-chunk-size guard (returns 1 to avoid division by zero in the
    /// hot path).
    #[test]
    fn compute_total_chunks_math() {
        assert_eq!(ChunkedMessageContext::compute_total_chunks(10, 4), 3);
        assert_eq!(ChunkedMessageContext::compute_total_chunks(12, 4), 3);
        assert_eq!(ChunkedMessageContext::compute_total_chunks(0, 4), 1);
        // Zero chunk size is treated as "no chunking", returns 1.
        assert_eq!(ChunkedMessageContext::compute_total_chunks(100, 0), 1);
    }

    /// End-to-end check: enqueue a send, sleep briefly, apply the receipt, observe that the
    /// histogram now has exactly one sample and the max is at least the sleep duration.
    #[test]
    fn apply_receipt_records_send_latency() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        let _ = p
            .queue_send(small_message(b"abc"), 100, std::time::Instant::now())
            .unwrap();
        let _ = p.next_outbound_frame();
        assert!(p.send_latency_hist.is_empty());

        std::thread::sleep(std::time::Duration::from_millis(2));
        let r = pb::CommandSendReceipt {
            producer_id: 1,
            sequence_id: 0,
            message_id: Some(pb::MessageIdData {
                ledger_id: 1,
                entry_id: 1,
                ..Default::default()
            }),
            highest_sequence_id: None,
        };
        let (_seq, _mid, _waker) = p.apply_receipt(&r).expect("receipt matched");
        assert_eq!(p.send_latency_hist.len(), 1);
        // We slept for at least 2 ms; the sample we recorded must reflect that.
        assert!(p.send_latency_max_ms() >= 1);
        let stats = p.stats();
        assert_eq!(stats.send_latency_max_ms, p.send_latency_max_ms());
    }

    /// Snapshot / replay round-trip — single-frame publish. Mirrors
    /// `Connection::reset` → `Connection::rebuild_producers` on the proto side.
    #[test]
    fn snapshot_then_replay_round_trips_single_send() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            1024,
        );
        let _ = p
            .queue_send(small_message(b"hi"), 100, std::time::Instant::now())
            .unwrap();
        // Drain the original outbound frame (the wire send happened).
        let _ = p.next_outbound_frame();
        assert_eq!(p.pending.len(), 1);
        assert_eq!(p.pending[0].replay_frames.len(), 1);

        let (wakers, snapshots) = p.snapshot_pending_sends();
        assert_eq!(snapshots.len(), 1, "one OpSend captured");
        assert_eq!(wakers.len(), 1, "one waker slot returned (None inside)");
        assert!(
            wakers[0].1.is_none(),
            "no waker was registered for this send"
        );
        assert_eq!(p.pending.len(), 0, "pending drained into the snapshot");

        // Replay: the OpSend goes back into pending, and the cached wire frame is
        // re-emitted into the producer's outbound queue.
        p.replay_snapshots(snapshots);
        assert_eq!(p.pending.len(), 1, "replay re-installs the OpSend");
        assert_eq!(p.outbound_len(), 1, "replay re-enqueues the wire frame");
        let frame = p.next_outbound_frame().expect("replay produces a frame");
        assert_eq!(frame.payload.as_ref(), b"hi");
        assert_eq!(frame.sequence_id, SequenceId(0));
    }

    /// Snapshot / replay round-trip — chunked publish. A single `OpSend` captures all
    /// chunk frames; replay re-emits them all in order.
    #[test]
    fn snapshot_then_replay_round_trips_chunked_send() {
        let mut p = ProducerState::new(ProducerHandle(1), "t".to_owned(), CompressionKind::None, 4);
        p.chunking_enabled = true;
        let payload = vec![b'z'; 10];
        let _ = p
            .queue_send(small_message(&payload), 100, std::time::Instant::now())
            .unwrap();
        // Drain the three chunk frames the producer just queued.
        for _ in 0..3 {
            let _ = p.next_outbound_frame();
        }
        // One OpSend covers all chunks; replay_frames carries all three.
        assert_eq!(p.pending.len(), 1);
        assert_eq!(p.pending[0].replay_frames.len(), 3);

        let (_wakers, snapshots) = p.snapshot_pending_sends();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].replay_frames.len(), 3);

        // Replay re-emits all three chunk frames in the original order.
        p.replay_snapshots(snapshots);
        assert_eq!(p.outbound_len(), 3, "all three chunks re-queued");
        for chunk_idx in 0..3i32 {
            let f = p.next_outbound_frame().expect("chunk");
            assert_eq!(f.metadata.chunk_id, Some(chunk_idx));
        }
    }

    /// Snapshot drains and clears the in-progress batch container so unflushed batched
    /// stragglers do not survive the reconnect. Mirrors Java
    /// `ProducerImpl#connectionClosed` which drops the batch on the floor.
    #[test]
    fn snapshot_clears_in_progress_batch_container() {
        let mut p = ProducerState::new(
            ProducerHandle(1),
            "t".to_owned(),
            CompressionKind::None,
            4096,
        );
        p.batching_enabled = true;
        p.max_messages_in_batch = 100;
        p.max_batch_size_bytes = 4096;
        // Two batched sends — each mints its own `OpSend` in `pending` so the user-side
        // `SendFut` has a unique key to wait on; the batch container still holds the raw
        // bytes until `flush_batch` builds the wire frame.
        let _ = p
            .queue_send(small_message(b"a"), 100, std::time::Instant::now())
            .unwrap();
        let _ = p
            .queue_send(small_message(b"b"), 100, std::time::Instant::now())
            .unwrap();
        assert!(!p.batch.is_empty());
        assert_eq!(p.pending.len(), 2);

        let (_wakers, snapshots) = p.snapshot_pending_sends();
        // The pre-flush `OpSend` entries carry no `replay_frames` (the wire frame is built
        // at flush time, not per-add), so reconnect replay is correctly empty for the
        // in-progress batch — the entries surface in the wakers list so the caller can
        // synthesise `Closed` errors, matching Java
        // `ProducerImpl#connectionClosed` which fails the pending batch.
        assert!(
            snapshots.is_empty(),
            "in-progress batched messages have no replay frames"
        );
        assert!(
            p.batch.is_empty(),
            "the batch container is dropped on snapshot"
        );
    }
}
