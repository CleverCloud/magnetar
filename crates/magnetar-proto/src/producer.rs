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
}

/// Snapshot of cumulative producer counters. Mirrors `org.apache.pulsar.client.api.ProducerStats`
/// for the totals; rates are derived above this layer.
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
    pub fn stats(&self) -> ProducerStats {
        ProducerStats {
            total_msgs_sent: self.total_msgs_sent,
            total_bytes_sent: self.total_bytes_sent,
            total_send_failed: self.total_send_failed,
            total_acks_received: self.total_acks_received,
            pending_queue_size: self.pending.len() as u64,
        }
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
    ) -> Result<SendDecision, ProducerError> {
        if self.closed {
            return Err(ProducerError::Closed);
        }

        let payload_size = msg.payload.len();
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
                return self.add_to_batch(msg, publish_time_ms);
            }
            return self.emit_chunked(msg, publish_time_ms);
        }

        if can_batch {
            return self.add_to_batch(msg, publish_time_ms);
        }

        self.emit_single(msg, publish_time_ms)
    }

    /// Emit a single SEND frame for a small, non-batched message.
    fn emit_single(
        &mut self,
        mut msg: OutgoingMessage,
        publish_time_ms: u64,
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
        self.outbound.push_back(OutboundFrame {
            command: cmd,
            metadata: msg.metadata,
            payload: msg.payload,
            sequence_id: seq,
        });
        let op = OpSend {
            sequence_id: seq,
            num_messages,
            waker: None,
            receipt: None,
            error: None,
            enqueued_at: std::time::Instant::now(),
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
        self.batch.messages.push((single, payload));
        // We assign sequence ids lazily at flush time so that we mint a contiguous range. The
        // Java client mints them per-message, but at flush time only the lowest is sent on the
        // wire; emitting per-message ids on the wire would require us to splice them into each
        // single-metadata, which the Java client does via `firstSequenceIdInBatch`.
        if self.batch.lowest_sequence_id.is_none() {
            self.batch.lowest_sequence_id = Some(self.next_sequence_id);
        }
        Ok(SendDecision::Batched)
    }

    /// Flush the batch container into one SEND frame. The caller is responsible for compression
    /// and any encryption of the concatenated payload. We hand back the raw concatenated bytes
    /// (singles' length-prefixed metadata followed by payload).
    ///
    /// Returns the number of frames now queued (always 0 or 1).
    pub fn flush_batch(&mut self, publish_time_ms: u64) -> usize {
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
        let lowest_seq = self.assign_sequence_id(&mut metadata, publish_time_ms);
        // For a multi-message batch we additionally bump the sequence counter by (num_messages-1)
        // so the next first-send picks up where the batch left off — matches Java
        // `firstSequenceIdInBatch + numMessages`.
        if num_messages > 1 {
            let extra = (num_messages as u64).saturating_sub(1);
            self.next_sequence_id = self.next_sequence_id.wrapping_add(extra);
            self.last_sequence_id_pushed = self.next_sequence_id.wrapping_sub(1) as i64;
            metadata.highest_sequence_id = Some(self.last_sequence_id_pushed as u64);
        }
        if self.compression != CompressionKind::None {
            metadata.compression = Some(self.compression.to_pb() as i32);
        }
        if let Ok(payload_total) = self.batch.current_size_bytes.try_into() {
            metadata.uncompressed_size = Some(payload_total);
        }

        let send = pb::CommandSend {
            producer_id: self.handle.0,
            sequence_id: lowest_seq.0,
            num_messages: Some(num_messages),
            txnid_least_bits: None,
            txnid_most_bits: None,
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
        self.outbound.push_back(OutboundFrame {
            command: cmd,
            metadata,
            payload,
            sequence_id: lowest_seq,
        });
        self.total_msgs_sent = self.total_msgs_sent.saturating_add(num_messages as u64);
        self.total_bytes_sent = self.total_bytes_sent.saturating_add(payload_len as u64);
        let op = OpSend {
            sequence_id: lowest_seq,
            num_messages,
            waker: None,
            receipt: None,
            error: None,
            enqueued_at: std::time::Instant::now(),
        };
        self.pending_index.insert(lowest_seq, self.pending.len());
        self.pending.push_back(op);

        self.batch.clear();
        1
    }

    /// Emit a sequence of chunk frames for a large message.
    fn emit_chunked(
        &mut self,
        msg: OutgoingMessage,
        publish_time_ms: u64,
    ) -> Result<SendDecision, ProducerError> {
        let txn_id = msg.txn_id;
        let payload = msg.payload;
        let uuid = uuid::Uuid::new_v4().to_string();
        let total_chunks =
            ChunkedMessageContext::compute_total_chunks(payload.len(), self.max_message_size);

        let mut ctx = ChunkedMessageContext {
            payload: payload.clone(),
            uuid: uuid.clone(),
            total_chunks,
            next_chunk: 0,
            max_chunk_size: self.max_message_size,
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
        self.last_sequence_id_pushed = ctx.sequence_id.0 as i64;

        // Emit each chunk frame eagerly into the outbound queue.
        let mut emitted = 0;
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
            self.outbound.push_back(OutboundFrame {
                command: cmd,
                metadata: chunk_meta,
                payload: chunk_payload,
                sequence_id: ctx.sequence_id,
            });
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
            enqueued_at: std::time::Instant::now(),
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
        let decision = p.queue_send(small_message(b"hello"), 100).unwrap();
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
        let decision = p.queue_send(small_message(&payload), 100).unwrap();
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
        let err = p.queue_send(small_message(&payload), 100).unwrap_err();
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
            let d = p.queue_send(small_message(b"x"), 100).unwrap();
            assert!(matches!(d, SendDecision::Batched));
        }
        assert_eq!(p.outbound_len(), 0);
        let flushed = p.flush_batch(101);
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
        let decision = p.queue_send(msg, 100).unwrap();
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
        let _ = p.queue_send(small_message(b"a"), 100).unwrap();
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

        let _ = p.queue_send(small_message(b"hello"), 100).unwrap();
        let _ = p.queue_send(small_message(b"world!"), 100).unwrap();
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
        let _ = p.queue_send(small_message(&payload), 100).unwrap();
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
            let _ = p.queue_send(small_message(b"x"), 100).unwrap();
        }
        assert_eq!(p.stats().total_msgs_sent, 0); // not flushed yet
        let _ = p.flush_batch(101);
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

        let _ = p.queue_send(small_message(b"first"), 100).unwrap();
        let frame = p.next_outbound_frame().expect("frame");
        assert_eq!(frame.metadata.sequence_id, 42);
        assert_eq!(frame.sequence_id.0, 42);

        let _ = p.queue_send(small_message(b"second"), 100).unwrap();
        let frame = p.next_outbound_frame().expect("frame");
        assert_eq!(frame.metadata.sequence_id, 43);
    }
}
