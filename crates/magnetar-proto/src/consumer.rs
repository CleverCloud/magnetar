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

use crate::error::ConsumerError;
use crate::event::IncomingMessage;
use crate::pb;
use crate::types::{ConsumerHandle, MessageId, RequestId};

/// Per-consumer state.
#[derive(Debug)]
pub struct ConsumerState {
    /// Consumer id assigned by [`Connection`](crate::Connection).
    pub handle: ConsumerHandle,
    /// Topic name.
    pub topic: String,
    /// Subscription name.
    pub subscription: String,
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
    /// Waker for the next `poll_receive`-style future.
    pub receive_waker: Option<Waker>,
    /// Closed flag.
    pub closed: bool,
    /// Configured max redelivery before DLQ routing kicks in (`0` disables DLQ routing).
    pub max_redeliver_count: u32,
    /// Messages flagged for DLQ routing. The runtime crate drains this and republishes.
    pub dead_letter_pending: Vec<IncomingMessage>,
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
            receiver_queue_size,
            available_permits: 0,
            consumed_since_flow: 0,
            queue: VecDeque::new(),
            chunk_reassembly: HashMap::new(),
            pending_seek: None,
            receive_waker: None,
            closed: false,
            max_redeliver_count: 0,
            dead_letter_pending: Vec::new(),
        }
    }

    /// Returns a `CommandFlow` if the consumer is below half of its receiver queue and not in
    /// a frozen state. Resets the consumed counter.
    pub fn maybe_flow(&mut self) -> Option<pb::CommandFlow> {
        if self.closed || self.pending_seek.is_some() {
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
    pub fn pop_message(&mut self) -> Option<IncomingMessage> {
        let msg = self.queue.pop_front()?;
        self.consumed_since_flow = self.consumed_since_flow.saturating_add(1);
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
    ) -> Result<DeliverOutcome, ConsumerError> {
        if self.closed {
            return Err(ConsumerError::Closed);
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
                };
                let trigger = self.classify_and_queue(im, redelivery_count);
                return Ok(trigger);
            }
        }

        // Batched message path.
        let num_in_batch = metadata.num_messages_in_batch.unwrap_or(1);
        if num_in_batch > 1 {
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
                };
                self.classify_and_queue(im, redelivery);
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
        };
        let outcome = self.classify_and_queue(im, redelivery);
        self.wake_receivers();
        Ok(outcome)
    }

    /// Route an [`IncomingMessage`] to the queue or the DLQ pending list. Returns the
    /// `DeliverOutcome::Delivered` count.
    fn classify_and_queue(&mut self, msg: IncomingMessage, redelivery: u32) -> DeliverOutcome {
        if self.max_redeliver_count > 0 && redelivery > self.max_redeliver_count {
            self.dead_letter_pending.push(msg);
            DeliverOutcome::Buffered
        } else {
            self.queue.push_back(msg);
            DeliverOutcome::Delivered {
                count: self.queue.len(),
            }
        }
    }

    fn wake_receivers(&mut self) {
        if let Some(w) = self.receive_waker.take() {
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

    /// Register a waker that fires when a new message arrives.
    pub fn register_receive_waker(&mut self, waker: Waker) {
        self.receive_waker = Some(waker);
    }

    /// Mark the consumer closed.
    pub fn close(&mut self) {
        self.closed = true;
        if let Some(w) = self.receive_waker.take() {
            w.wake();
        }
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
            c.deliver(&message_cmd(0), metadata(1), None, Bytes::from_static(b"x"))
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
            .deliver(&message_cmd(0), metadata(2), None, buf.freeze())
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
            let outcome = c.deliver(&message_cmd(0), meta, None, body).unwrap();
            // The first two are buffered; the third triggers delivery.
            match outcome {
                DeliverOutcome::Buffered | DeliverOutcome::Delivered { .. } => {}
                other => panic!("unexpected outcome: {other:?}"),
            }
        }
        let msg = c.pop_message().expect("reassembled message");
        assert_eq!(msg.payload.as_ref(), b"aabbcc");
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
            )
            .unwrap();
        assert!(c.queue.is_empty());
        assert_eq!(c.dead_letter_pending.len(), 1);
    }
}
