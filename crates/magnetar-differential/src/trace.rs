// SPDX-License-Identifier: Apache-2.0

//! Trace data model.
//!
//! A [`Trace`] is a sequence of producer/consumer [`Op`]s the harness
//! replays against an engine. Each op resolves to one [`Event`] in the
//! returned [`EventStream`], so traces and event streams are aligned
//! 1:1 by index. The harness then byte-compares the two event streams
//! produced by the tokio and moonpool runners.
//!
//! Op surface is intentionally tight: `Send`, `Recv`, `Ack`, `Nack`,
//! `Seek`, `Close`, plus partition-aware siblings `SendPartition`,
//! `RecvPartition`, `AckPartition`, `SeekPartition` for the
//! partitioned-topic traces. Extend it as new differential coverage
//! lands; keep every variant **observable** so the equivalence check
//! stays meaningful.

use std::time::Duration;

use magnetar_proto::MessageId;

/// A single operation in a [`Trace`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Send a single message with `payload` bytes. The harness wraps
    /// it in a default [`magnetar_proto::producer::OutgoingMessage`]
    /// (no compression, no transaction, no partition key).
    Send {
        /// Raw payload bytes (uncompressed, unencrypted).
        payload: Vec<u8>,
    },
    /// Receive one message with the given timeout. The harness waits
    /// up to `timeout` for a message to arrive on the consumer's
    /// per-consumer queue before returning [`Event::RecvTimeout`].
    Recv {
        /// How long to wait before surfacing [`Event::RecvTimeout`].
        timeout: Duration,
    },
    /// Individually acknowledge a single message id. Resolves once
    /// the scripted broker emits its `CommandAckResponse`.
    Ack {
        /// Target message id.
        message_id: MessageId,
    },
    /// Negatively acknowledge a single message id. Fire-and-forget at
    /// the engine surface, but the scripted broker observes it and
    /// re-pushes the message; the next `Recv` should see it.
    Nack {
        /// Target message id.
        message_id: MessageId,
    },
    /// Seek the consumer to a specific message id. The broker replays
    /// from there on the next push tick.
    Seek {
        /// Target message id (cursor reset point).
        message_id: MessageId,
    },
    /// Close the producer (when the consumer hasn't been opened, this
    /// is a no-op on the consumer side) and the consumer if open.
    /// Resolves the producer/consumer close round-trip.
    Close,
    /// Send to a specific partition of [`Trace::topic`]. Internally the
    /// runner opens (or reuses) a producer bound to
    /// `<trace.topic>-partition-N`, mirroring Java's
    /// `PartitionedProducerImpl` topic-naming convention.
    SendPartition {
        /// Zero-based partition index. The runner resolves it to the
        /// per-partition topic name suffix `-partition-{partition}`.
        partition: i32,
        /// Raw payload bytes (uncompressed, unencrypted).
        payload: Vec<u8>,
    },
    /// Receive one message from a specific partition. Mirrors
    /// [`Op::Recv`] but targets the per-partition consumer.
    RecvPartition {
        /// Zero-based partition index.
        partition: i32,
        /// How long to wait before surfacing
        /// [`Event::RecvTimeoutPartition`].
        timeout: Duration,
    },
    /// Individually acknowledge a single message id on the given
    /// partition's consumer.
    AckPartition {
        /// Zero-based partition index.
        partition: i32,
        /// Target message id.
        message_id: MessageId,
    },
    /// Seek the per-partition consumer to a specific message id. The
    /// scripted broker resets the cursor on **only** that partition's
    /// ledger; other partitions keep their current cursor.
    SeekPartition {
        /// Zero-based partition index.
        partition: i32,
        /// Target message id (cursor reset point).
        message_id: MessageId,
    },
    /// PIP-180 / ADR-0033: replicator-style send that propagates a
    /// source-topic `MessageId` on the wire (`CommandSend.message_id`).
    /// The scripted broker echoes the asserted id back on
    /// `CommandSendReceipt` (round-trip preservation), so the resulting
    /// [`Event::Sent`]'s `message_id` MUST equal `source_msg_id` on
    /// both engines — that's the differential equivalence claim.
    SendWithSourceId {
        /// Source-topic `MessageId` to assert on the send.
        source_msg_id: MessageId,
        /// Raw payload bytes (uncompressed, unencrypted).
        payload: Vec<u8>,
    },
    /// PIP-31: open a transaction at the broker-side transaction
    /// coordinator. On success the runner stores the returned
    /// [`magnetar_proto::TxnId`] for the next [`Self::EndTxn`] op. The
    /// harness supports one in-flight transaction at a time per trace.
    NewTxn {
        /// Transaction timeout in milliseconds. The TC fails the
        /// transaction if the client doesn't end it within this window.
        timeout_ms: u64,
    },
    /// PIP-31: commit or abort the open transaction (the one returned
    /// by the most recent [`Self::NewTxn`] op). The scripted broker
    /// drains the per-txn ack ledger on commit; drops it on abort.
    EndTxn {
        /// `true` → commit; `false` → abort.
        commit: bool,
    },
    /// PIP-31: publish a single message stamped with the currently-open
    /// transaction id (set by the most recent [`Self::NewTxn`]). The
    /// runner stamps the `OutgoingMessage::txn_id` field; the broker
    /// observes the send the same way it observes a non-txn publish
    /// (the staged-ack ledger only tracks acks, not sends, in our
    /// scripted model — the real broker would route the send to the
    /// txn's per-partition pending entries). With no open txn, the
    /// runner emits [`Event::SendInTxnError`] without contacting the
    /// broker. Mirrors Java
    /// `Producer#newMessage(Transaction).value(...).send()`.
    SendInTxn {
        /// Raw payload bytes (uncompressed, unencrypted).
        payload: Vec<u8>,
    },
    /// PIP-31: acknowledge a single message id against the currently-
    /// open transaction id (set by the most recent [`Self::NewTxn`]).
    /// The scripted broker stages the ack against the per-txn ledger
    /// keyed by `(most, least)`; the staged acks drain on commit and
    /// drop on abort. With no open txn, the runner emits
    /// [`Event::AckInTxnError`] without contacting the broker. Mirrors
    /// Java `Consumer#acknowledgeAsync(MessageId, Transaction)`.
    AckInTxn {
        /// Target message id.
        message_id: MessageId,
    },
}

/// Outcome of one [`Op`]. Returned positionally — `Trace::ops[i]`
/// resolves to `EventStream::events[i]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// `Send` succeeded; broker assigned [`MessageId`].
    Sent {
        /// Sequence id the engine surfaced on success.
        message_id: MessageId,
    },
    /// `Send` failed at the engine surface (e.g. closed connection).
    SendError {
        /// Human-readable error category. The harness collapses the
        /// full error to a stable string so the two engines compare
        /// equal even when their `Display` impls differ in punctuation.
        kind: String,
    },
    /// `Recv` returned a message. `payload` and `message_id` must
    /// match across engines.
    Received {
        /// Payload bytes the broker pushed.
        payload: Vec<u8>,
        /// Broker-assigned message id.
        message_id: MessageId,
    },
    /// `Recv` timed out without a message arriving.
    RecvTimeout,
    /// `Ack` was acknowledged by the broker.
    Acked,
    /// `Ack` failed at the engine surface or was rejected by the
    /// broker. Same `kind` collapse as [`Event::SendError`].
    AckError {
        /// Stable error category string.
        kind: String,
    },
    /// `Nack` was enqueued (fire-and-forget at the engine surface).
    /// The redelivery itself surfaces as a follow-up [`Event::Received`].
    Nacked,
    /// `Seek` succeeded.
    Seeked,
    /// `Seek` failed.
    SeekError {
        /// Stable error category string.
        kind: String,
    },
    /// `Close` completed for the producer and (if open) consumer.
    Closed,
    /// `SendPartition` succeeded; broker assigned [`MessageId`] on the
    /// given partition.
    SentPartition {
        /// Zero-based partition index the send was routed to.
        partition: i32,
        /// Sequence id the engine surfaced on success.
        message_id: MessageId,
    },
    /// `RecvPartition` returned a message.
    ReceivedPartition {
        /// Zero-based partition index the recv pulled from.
        partition: i32,
        /// Payload bytes the broker pushed.
        payload: Vec<u8>,
        /// Broker-assigned message id.
        message_id: MessageId,
    },
    /// `RecvPartition` timed out without a message arriving.
    RecvTimeoutPartition {
        /// Zero-based partition index the recv was bound to.
        partition: i32,
    },
    /// `AckPartition` succeeded.
    AckedPartition {
        /// Zero-based partition index the ack targeted.
        partition: i32,
    },
    /// `SeekPartition` succeeded — only the given partition's cursor
    /// was reset.
    SeekedPartition {
        /// Zero-based partition index whose cursor was reset.
        partition: i32,
    },
    /// `NewTxn` succeeded. The harness suppresses the broker-allocated
    /// txn id from the event payload (it's allocated by the scripted
    /// broker so the two engines may disagree on the exact bits if
    /// they're observed in different order). The Sent event is
    /// sufficient to assert "the txn open round-trip resolved" — the
    /// differential equivalence claim is on the event sequence, not
    /// on the txn-id bits.
    TxnCreated,
    /// `NewTxn` failed at the engine surface or broker.
    TxnCreateError {
        /// Stable error category string.
        kind: String,
    },
    /// `EndTxn` succeeded. The `committed` field mirrors the input op
    /// (`true` → commit; `false` → abort) so the event carries the
    /// outcome shape directly.
    TxnEnded {
        /// `true` → commit was acked; `false` → abort was acked.
        committed: bool,
    },
    /// `EndTxn` failed at the engine surface or broker.
    TxnEndError {
        /// Stable error category string.
        kind: String,
    },
    /// `SendInTxn` succeeded; broker assigned [`MessageId`]. Mirrors
    /// [`Event::Sent`] — the txn id is intentionally not surfaced (it's
    /// broker-allocated and not part of the differential equivalence
    /// claim; the drain assertion runs against the broker's per-txn
    /// ledger snapshot, see
    /// [`crate::broker::ScriptedBroker::txn_drain_log_snapshot`]).
    SentInTxn {
        /// Sequence id the engine surfaced on success.
        message_id: MessageId,
    },
    /// `SendInTxn` was attempted with no open transaction, or failed at
    /// the engine surface / broker.
    SendInTxnError {
        /// Stable error category string.
        kind: String,
    },
    /// `AckInTxn` was acknowledged by the broker as staged against the
    /// open transaction. The drain/drop semantics surface on
    /// `EndTxn(commit|abort)` via
    /// [`crate::broker::ScriptedBroker::txn_drain_log_snapshot`].
    AckedInTxn,
    /// `AckInTxn` was attempted with no open transaction, or failed at
    /// the engine surface / broker.
    AckInTxnError {
        /// Stable error category string.
        kind: String,
    },
}

/// A scripted sequence of [`Op`]s the harness replays against an
/// engine. The harness opens **one** producer on `topic` and **one**
/// consumer on `(topic, subscription)`. More elaborate fan-out
/// (multiple producers, partitioned topics, multi-subscription) lands
/// as follow-up scope.
#[derive(Debug, Clone)]
pub struct Trace {
    /// Topic name used for the producer and consumer.
    pub topic: String,
    /// Subscription name used for the consumer.
    pub subscription: String,
    /// Ordered ops to replay.
    pub ops: Vec<Op>,
}

impl Trace {
    /// Convenience constructor.
    #[must_use]
    pub fn new(topic: impl Into<String>, subscription: impl Into<String>, ops: Vec<Op>) -> Self {
        Self {
            topic: topic.into(),
            subscription: subscription.into(),
            ops,
        }
    }
}

/// Per-trace output: one [`Event`] per [`Op`], in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventStream {
    /// Aligned 1:1 with the input [`Trace::ops`].
    pub events: Vec<Event>,
}

impl EventStream {
    /// Construct an empty stream — useful when a runner aborts early
    /// (the equivalence checker will catch the length mismatch).
    #[must_use]
    pub fn empty() -> Self {
        Self { events: Vec::new() }
    }

    /// Number of events recorded so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// `true` when no events have been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Append one event.
    pub fn push(&mut self, event: Event) {
        self.events.push(event);
    }
}

impl Default for EventStream {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use magnetar_proto::MessageId;

    use super::{Event, EventStream, Op, Trace};

    fn mid(ledger: u64, entry: u64) -> MessageId {
        MessageId {
            ledger_id: ledger,
            entry_id: entry,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
        }
    }

    #[test]
    fn trace_round_trip() {
        let t = Trace::new(
            "persistent://public/default/t",
            "s",
            vec![
                Op::Send {
                    payload: b"hi".to_vec(),
                },
                Op::Recv {
                    timeout: Duration::from_secs(1),
                },
                Op::Ack {
                    message_id: mid(1, 0),
                },
                Op::Close,
            ],
        );
        assert_eq!(t.ops.len(), 4);
        assert_eq!(t.topic, "persistent://public/default/t");
        assert_eq!(t.subscription, "s");
    }

    #[test]
    fn event_stream_push() {
        let mut s = EventStream::empty();
        assert!(s.is_empty());
        s.push(Event::Sent {
            message_id: mid(1, 0),
        });
        s.push(Event::Acked);
        assert_eq!(s.len(), 2);
        assert!(matches!(s.events[1], Event::Acked));
    }

    /// `EventStream` equality is the comparison the harness relies on —
    /// confirm that the two streams compare equal byte-for-byte when
    /// constructed from the same inputs.
    #[test]
    fn event_stream_equality() {
        let a = EventStream {
            events: vec![Event::Sent {
                message_id: MessageId {
                    ledger_id: 1,
                    entry_id: 2,
                    partition: -1,
                    batch_index: -1,
                    batch_size: 0,
                },
            }],
        };
        let b = EventStream {
            events: vec![Event::Sent {
                message_id: MessageId {
                    ledger_id: 1,
                    entry_id: 2,
                    partition: -1,
                    batch_index: -1,
                    batch_size: 0,
                },
            }],
        };
        assert_eq!(a, b);
    }
}
