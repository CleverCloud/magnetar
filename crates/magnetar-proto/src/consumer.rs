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

/// Immutable per-consumer metadata, set at subscribe time and never mutated.
/// Held inside [`ConsumerSlot`] so cold-path observers can read it without
/// taking the slot's mutex.
///
/// Mirrors the set of `ConsumerImpl` fields that are stamped at construction
/// from the user-supplied `ConsumerBuilder` and never re-assigned: topic,
/// subscription, and the caller-visible consumer handle.
#[derive(Debug, Clone)]
pub struct ConsumerIdentity {
    /// Consumer id assigned by the connection at subscribe time.
    pub handle: ConsumerHandle,
    /// Topic the consumer is subscribed to.
    pub topic: String,
    /// Subscription name.
    pub subscription: String,
}

/// Per-consumer slot: immutable identity plus mutex-guarded state.
///
/// `Arc<ConsumerSlot>` is the long-lived handle the runtime engines store on
/// their `Consumer` value, AND the value that [`crate::Connection`] keeps in
/// its consumer registry — both ends hold the same `Arc`. Cold-path
/// observability (topic, subscription, handle) reads
/// [`ConsumerSlot::identity`] without locking; mutable operations take
/// [`ConsumerSlot::state`].lock()`.
///
/// # Lock-ordering invariant (project-wide)
///
/// `Connection` is wrapped in a `parking_lot::Mutex` by the runtime engines
/// (see `magnetar-runtime-tokio::ConnectionShared::inner`); every
/// `ConsumerSlot` carries its own `parking_lot::Mutex<ConsumerState>`. The
/// allowed acquisition order is:
///
/// 1. **Global Connection mutex → per-slot mutex** is safe.
/// 2. **Per-slot mutex → global Connection mutex is FORBIDDEN.** A holder of `slot.state.lock()`
///    that needs `Connection`-level state must release the slot lock first.
///
/// Violating this rule will deadlock under contention.
///
/// See [ADR-0038 — Split Connection Mutex](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0038-split-connection-mutex.md).
#[derive(Debug)]
pub struct ConsumerSlot {
    /// Immutable identifying metadata. Safe to read without locking.
    pub identity: ConsumerIdentity,
    /// Mutex-guarded state-machine state. Hot path for queue / waker /
    /// flow-control operations.
    pub state: parking_lot::Mutex<ConsumerState>,
}

impl ConsumerSlot {
    /// Construct a slot for a newly-subscribed consumer.
    #[must_use]
    pub fn new(identity: ConsumerIdentity, state: ConsumerState) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            identity,
            state: parking_lot::Mutex::new(state),
        })
    }
}

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
// reason: `closed` / `paused` / `reached_end_of_topic` /
// `closed` / `paused` / `reached_end_of_topic` are orthogonal protocol axes
// (user latch, user toggle, broker signal), not an encodable state machine —
// collapsing them into enums would invent product states that cannot occur.
#[allow(clippy::struct_excessive_bools)]
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
    /// Current receiver queue target — the consumer asks the broker for permits in batches of
    /// `receiver_queue_size / 2` once half of the queue has been consumed.
    ///
    /// Issue #301: this is no longer the immutable user setting. It is the
    /// *current* target produced by [`Self::policy`]: seeded from
    /// `policy.initial()` at subscribe time, and recomputed each adjust tick by
    /// `policy.adjust(&FlowStats)`. For the default [`crate::receiver_queue::Fixed`]
    /// policy it never changes, so behaviour is identical to the raw-`usize`
    /// design; an [`crate::receiver_queue::Auto`] policy ramps it under load.
    pub receiver_queue_size: usize,
    /// Pluggable receiver-queue-size policy (issue #301). Holds only immutable
    /// configuration — all mutable target state lives in
    /// [`Self::receiver_queue_size`]. `Arc<dyn>` so a subscribe replay clones the
    /// handle, never the policy. Default is
    /// [`crate::receiver_queue::Fixed`]`(receiver_queue_size)`, preserving the
    /// pre-#301 behaviour exactly.
    ///
    /// `policy.adjust` is pure (no clock/RNG/I/O) so the tokio and moonpool
    /// engines stay bit-reproducible — see the module docs on
    /// [`crate::receiver_queue`].
    pub policy: std::sync::Arc<dyn crate::receiver_queue::ReceiverQueuePolicy>,
    /// Running total of `payload.len()` across every message currently buffered
    /// in [`Self::queue`] (issue #301). Maintained in lock-step: bumped on
    /// enqueue in [`Self::classify_and_queue`], decremented on dequeue in
    /// [`Self::pop_message`]. This is the `in_flight_bytes` signal the
    /// [`crate::receiver_queue::Auto`] OOM guard bounds; it is distinct from
    /// [`Self::chunk_buffered_bytes`] (which counts *incomplete* chunk
    /// reassembly buffers, not delivered queue entries).
    pub(crate) queued_bytes: u64,
    /// Minimum spacing between [`crate::receiver_queue::ReceiverQueuePolicy::adjust`]
    /// ticks, driven from [`crate::Connection::handle_timeout`] (issue #301).
    /// `None` disables auto-adjust entirely (the default for the
    /// [`crate::receiver_queue::Fixed`] policy — no point ticking a constant).
    /// `Some(d)` schedules the next adjust at `last_adjust_at + d`, surfaced via
    /// [`crate::Connection::poll_timeout`].
    pub(crate) adjust_interval: Option<std::time::Duration>,
    /// Injected-clock timestamp of the last adjust tick, or `None` before the
    /// first. Drives the [`Self::next_adjust_deadline`] schedule. Never
    /// `Instant::now()` (ADR-0011) — set from `handle_timeout`'s `now`.
    pub(crate) last_adjust_at: Option<std::time::Instant>,
    /// Cumulative permits GRANTED to the broker since the last zeroing (subscribe, reconnect
    /// reset, terminal subscribe failure, same-broker `CloseConsumer`). Issue #349: this is a
    /// purely ADDITIVE mirror — it is bumped at every grant site (`initial_flow`, `maybe_flow`,
    /// the growth branch of `adjust_receiver_queue`) but is NEVER decremented as messages
    /// actually arrive, so it does not track the broker's REAL outstanding grant. It is still
    /// the right register for "how much have we told the broker it may use": the #307
    /// failover-reflow gate and the `adjust_receiver_queue` want-have delta both need exactly
    /// that question answered, and this additive counter answers it correctly. For the REAL,
    /// decrementing balance — the starvation signal
    /// [`crate::receiver_queue::FlowStats::available_permits`] needs — see
    /// [`Self::permit_balance`].
    pub granted_permits: u32,
    /// REAL broker-side permit balance: `granted_permits` minus one unit per broker dispatch
    /// unit that has actually arrived (issue #349). Incremented at the same three grant sites
    /// as `granted_permits` (`initial_flow`, `maybe_flow`, `adjust_receiver_queue`'s growth
    /// branch) by the identical delta. Decremented by exactly one (saturating) per dispatch
    /// unit as it arrives:
    ///
    /// - Once per delivered logical message in `classify_and_queue` — covers a plain message, each
    ///   batch member, and the chunk-completing logical message. Unconditional across both the
    ///   queued and dead-lettered branches: the broker already spent one permit dispatching the
    ///   entry regardless of where the client routes it afterward.
    /// - Once per incomplete chunk buffered in [`Self::deliver`] (the chunk has not yet reached
    ///   `classify_and_queue` — reassembly is still pending — but the broker already dispatched
    ///   it).
    /// - Once per PIP-33 marker in [`Self::record_marker_consumed`].
    ///
    /// Force-zeroed everywhere `granted_permits` is zeroed so the two counters never drift apart
    /// at a churn boundary. `flow_stats` feeds this — not `granted_permits` — into
    /// [`crate::receiver_queue::FlowStats::available_permits`], so `0` is now a genuine
    /// starvation signal instead of the never-decrementing mirror `granted_permits` was before
    /// this split.
    pub permit_balance: u32,
    /// Monotonic count of broker dispatch units this consumer has observed since it
    /// was constructed (issue #414). Bumped by [`Self::record_dispatch_unit`], the single
    /// helper that also decrements [`Self::permit_balance`], so the two can never drift:
    /// one unit per plain message, per batch member, per buffered chunk, and per PIP-33
    /// marker.
    ///
    /// This is the stall watchdog's **progress signal**. It carries no clock, so the
    /// dispatch sites need no `now` parameter (ADR-0011): the watchdog compares the mark
    /// it latched at the start of a silence window against the current value, exactly the
    /// way ADR-0058's keepalive compares `last_activity` against the deadline it armed.
    /// Never reset — a wrapped `u64` would need ~584 years of continuous 1 GHz dispatch.
    pub(crate) dispatch_units_received: u64,
    /// Live per-consumer stall window (issue #414), or `None` when this consumer is not
    /// currently a stall candidate ([`Self::is_stall_candidate`]). Seeded, advanced, and
    /// cleared exclusively by [`Self::poll_stall`], which
    /// [`crate::Connection::handle_timeout`] drives; the matching deadline is surfaced
    /// through [`Self::next_stall_deadline`] so
    /// [`crate::Connection::poll_timeout`] schedules a deterministic wake.
    ///
    /// Cleared at every grant site too ([`Self::initial_flow`], [`Self::maybe_flow`],
    /// [`Self::adjust_receiver_queue`]'s growth branch): a fresh grant is a fresh promise
    /// from the broker and deserves a fresh silence window rather than inheriting the
    /// previous one's start instant.
    pub(crate) stall_watch: Option<StallWatch>,
    /// In-place re-subscribes the stall watchdog has driven for this consumer in the
    /// current stall streak (issue #414, ADR-0103). Compared against
    /// [`crate::ConnectionConfig::consumer_stall_auto_recovery`] in
    /// [`crate::Connection::handle_timeout`], and bumped only when a re-subscribe was
    /// actually emitted — a consumer the eligibility gate refuses spends no budget, and
    /// neither does one the standby pre-check skips ([`Self::is_active`] reported
    /// `Some(false)`, ADR-0103).
    ///
    /// Reset to zero in exactly ONE place: [`Self::record_dispatch_unit`], i.e. a broker
    /// dispatch unit genuinely arriving. Resetting it at the churn boundaries that zero
    /// the permit mirrors would be wrong rather than merely generous — the recovery's own
    /// [`crate::Connection::resubscribe_consumer_in_place`] is one of those boundaries, so
    /// the counter would clear itself on every attempt and the bound would not exist.
    pub(crate) stall_recovery_attempts: u32,
    /// Number of permits we've consumed since the last flow command. Visible to the
    /// [`Connection`](crate::Connection) so it can adjust the counter when surfacing messages
    /// to the user via `pop_message` paths that bypass `ConsumerState::pop_message`.
    pub(crate) consumed_since_flow: u32,
    /// Inbound queue of messages ready to deliver to the user.
    pub queue: VecDeque<IncomingMessage>,
    /// Per-uuid chunk reassembly state.
    chunk_reassembly: HashMap<String, ChunkBuffer>,
    /// FIFO insertion-order index over `chunk_reassembly`, mirroring Java
    /// `ConsumerImpl#pendingChunkedMessageUuidQueue`. The front is the oldest
    /// incomplete chunked message; eviction (`max_pending_chunked_message`
    /// breach) and the expiry sweep both pop from the front. Kept in lock-step
    /// with `chunk_reassembly`: every genuine first-chunk insert pushes the
    /// uuid here, and every removal (reassembly, eviction, expiry) drops it.
    chunk_reassembly_order: VecDeque<String>,
    /// Aggregate buffered chunk-payload bytes across every incomplete buffer.
    /// Bounded against the depth-axis DoS where a hostile broker advertises a
    /// huge `num_chunks_from_msg` and streams distinct chunk_ids into one
    /// buffer (see [`MAX_BUFFERED_CHUNK_BYTES`]).
    chunk_buffered_bytes: usize,
    /// Maximum number of distinct incomplete chunked messages buffered at once.
    /// Mirrors Java `ConsumerConfigurationData#maxPendingChunkedMessage`
    /// (default `10`). On breach, the oldest incomplete message is evicted
    /// (`removeOldestPendingChunkedMessage` parity). `0` disables the cap
    /// (matches Java's `> 0` guard).
    pub max_pending_chunked_message: usize,
    /// When the `max_pending_chunked_message` cap is breached, `true` acks the
    /// oldest partial message's first-chunk id before dropping it (the broker
    /// treats it as consumed); `false` (the default, matching Java
    /// `autoAckOldestChunkedMessageOnQueueFull`) drops it without acking so the
    /// broker eventually redelivers the whole message.
    pub auto_ack_oldest_chunked_message_on_queue_full: bool,
    /// Expiry window for an incomplete chunked message. Mirrors Java
    /// `expireTimeOfIncompleteChunkedMessageMillis` (default `60s`). A buffer
    /// older than this is swept in [`crate::Connection::handle_timeout`] and
    /// the wake is scheduled through [`crate::Connection::poll_timeout`].
    /// `None` disables the sweep (matches Java's `> 0` guard).
    pub expire_time_of_incomplete_chunked_message: Option<std::time::Duration>,
    /// First-chunk message ids of partial buffers evicted/expired with
    /// `auto_ack_oldest_chunked_message_on_queue_full = true`. Drained by
    /// [`crate::Connection`] (mirroring [`Self::dead_letter_pending`]) which
    /// emits the individual `CommandAck`. Empty in the default `auto_ack =
    /// false` path.
    pub chunk_auto_ack_pending: Vec<MessageId>,
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
    /// The unsubscribe request currently in flight, if any.
    ///
    /// This mirrors Java's `Closing` state closely enough for retry ownership:
    /// detached re-attachment legs must stop before they can enqueue a
    /// `CommandSubscribe` behind `CommandUnsubscribe`. A broker rejection
    /// clears the flag and restores the prior attachment generation.
    pub(crate) unsubscribe_request_id: Option<RequestId>,
    /// Whether this consumer has completed at least one broker attachment.
    /// Provisional retries belong to the routing-aware client operation;
    /// established-handle retries remain connection-local reattachment.
    pub(crate) has_ever_attached: bool,
    /// Number of consecutive retryable `CommandSubscribe` rejections
    /// returned for this consumer since the last success. Bumped by the
    /// [`crate::Connection`] error handler on each transient subscribe
    /// failure; reset to `0` when the re-subscribe is acked. The runtime
    /// drivers read it via
    /// [`crate::Connection::consumer_transient_subscribe_attempts`] to size
    /// their exponential-backoff sleep before the next lookup + retry, and the
    /// proto layer installs a terminal failure once it crosses
    /// [`crate::OperationRetryConfig::max_retries`] (issue #302 / ADR-0080).
    pub transient_subscribe_attempts: u32,
    /// Last retryable broker rejection observed while this consumer is
    /// subscribing. Runtime deadlines surface it instead of replacing useful
    /// broker diagnostics with a synthetic timeout.
    pub(crate) last_subscribe_error: Option<(i32, String)>,
    /// `Some(reason)` once this consumer has been given a TERMINAL subscribe
    /// failure — the transient-retry budget was exhausted (issue #302) or a
    /// non-recoverable subscribe error landed. Installed by
    /// [`crate::Connection::fail_consumer_subscribe`], which also wakes every
    /// parked `receive()` waker so the future resolves `Err` instead of
    /// hanging forever. The runtime receive futures gate on this (via
    /// [`crate::Connection::consumer_handle_is_terminal`]) to distinguish a
    /// genuinely-dead subscription from a transiently-`Failed` connection a
    /// supervisor is still reconnecting (issue #299 — a recoverable `Failed`
    /// window must NOT surface `Closed`).
    pub terminal_failure: Option<String>,
    /// Stable user-waiter token, initialized from the first subscribe request
    /// id and preserved while rebuild/retry replaces the active wire request.
    pub(crate) subscribe_waiter_id: Option<RequestId>,
    /// Active wire request owned by [`Self::subscribe_waiter_id`]. Older
    /// same-handle replies cannot satisfy the stable waiter.
    pub(crate) subscribe_waiter_request: Option<RequestId>,
    /// The active wire request succeeded, but the runtime has not yet consumed
    /// the stable waiter completion. Preserved across reset so rebuild can
    /// transfer the same waiter onto the replacement session.
    pub(crate) subscribe_waiter_completed: bool,
    /// Re-attach flow gate: set while a driver-owned subscribe waits for its
    /// acknowledgement. Kept public as a boolean for source compatibility;
    /// the correlated wire request is retained separately by the connection.
    pub flow_on_subscribe_ack: bool,
    /// Current driver-owned re-attach request whose acknowledgement releases
    /// initial flow. Request-id correlation prevents an older overlapping
    /// subscribe success from sending flow before the latest subscribe lands.
    pub(crate) flow_on_subscribe_ack_request: Option<RequestId>,
    /// A `CommandSubscribe` has been emitted for this consumer and the initial
    /// `CommandFlow` for that attach has not been issued yet (issue #427).
    ///
    /// Set by every `CommandSubscribe` emission — fresh subscribe, reconnect rebuild,
    /// transient-subscribe retry, post-seek resubscribe, in-place re-attach — and cleared
    /// by [`Self::initial_flow`], the single funnel every initial grant routes through.
    /// It is what lets [`crate::Connection::initial_flow`] grant **once** per attach: the
    /// broker (re-)creates its dispatcher slot at `availablePermits = 0`, so an attach with
    /// no grant yet always needs one, and an attach that already has one never needs a
    /// second.
    ///
    /// It is deliberately NOT the same question as `granted_permits == 0`. A post-seek
    /// resubscribe leaves the additive `granted_permits` mirror alone (nothing zeroes it on
    /// that path) while the broker's new dispatcher slot genuinely starts empty, so only
    /// this flag can tell that attach apart from a consumer that is already fed.
    pub(crate) initial_grant_due: bool,
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
    pub batch_ack_tracker: rustc_hash::FxHashMap<(u64, u64), BatchAckEntry>,
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
    /// measuring the interval between [`IncomingMessage::arrived_at`] (the moment the consumer
    /// state machine queued the message) and the `now` the caller passes to `pop_message`.
    /// Both ends are engine-injected instants, never host-clock reads (ADR-0011, ADR-0086), so
    /// the distribution is reproducible per seed under the moonpool engine. Mirrors the latency
    /// percentiles surfaced by Java `ConsumerStatsRecorder` (p50, p99, max). Three significant
    /// digits, auto-resizing.
    ///
    /// `Option`-typed so the constructor never has to `.expect(...)` on a statically-valid
    /// `hdrhistogram::Histogram::new(3)` (invariant #6). `None` means the histogram could
    /// not be initialised (impossible in any non-broken hdrhistogram build); stats helpers
    /// report zero percentiles when `None` or empty.
    pub receive_latency_hist: Option<hdrhistogram::Histogram<u64>>,
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
    /// (the default — wire byte-identical receive path). `Some(meta)` is
    /// injected by the runtime engine at subscribe time when the admin REST
    /// `getShadowTopics(source)` lookup resolves the consumer's topic as a
    /// shadow of another. The connection's receive dispatch (see
    /// [`crate::Connection::poll_event`]) reads this and emits the
    /// [`crate::event::ConnectionEvent::MessageReceivedFromShadow`] variant
    /// instead of the regular [`crate::event::ConnectionEvent::Message`]
    /// when the inbound entry's [`pb::MessageMetadata::replicated_from`] is
    /// also populated.
    pub shadow_metadata: Option<ShadowTopicMetadata>,
    /// Last broker-reported Failover active/standby state (issue #348). `None`
    /// until the first `CommandActiveConsumerChange` lands for this consumer;
    /// thereafter mirrors the most recent `is_active` the broker sent. Set by
    /// [`Self::record_active_change`], which also pushes the value onto
    /// [`Self::active_changes`] and wakes every parked
    /// [`Self::active_change_wakers`] entry. Mirrors Java
    /// `ConsumerEventListener#becameActive` / `becameInactive` — the runtime
    /// engines surface transitions via `Consumer::is_active` /
    /// `Consumer::next_active_change`.
    pub is_active: Option<bool>,
    /// Bounded FIFO of not-yet-observed active/standby transitions, capped at
    /// [`ACTIVE_CHANGES_CAP`] (oldest dropped on overflow — issue #348). Each
    /// runtime `next_active_change()` future pops one entry per resolution via
    /// [`Self::pop_active_change`]; [`Self::record_active_change`] pushes.
    /// Mirrors [`Self::queue`]'s bounded-buffer shape, but for the much
    /// lower-cardinality active-change signal (a Failover flip happens at most
    /// a handful of times per session, so 32 is a generous cap — this exists
    /// only to bound memory against a pathological broker, not because
    /// legitimate traffic ever approaches it).
    pub active_changes: VecDeque<bool>,
    /// Per-consumer waker slab for parked `next_active_change()` futures
    /// (issue #348). Mirrors [`Self::receive_wakers`] exactly: each in-flight
    /// future registers a `Waker` via [`Self::register_active_change_waker`]
    /// and evicts it on `Drop` via [`Self::cancel_active_change_waker`]; a
    /// new recorded transition (or a terminal close) drains and wakes every
    /// parked slot. Not a channel — a `Slab<Waker>` is the canonical
    /// no-channel wake pattern (ADR-0003).
    pub active_change_wakers: Slab<Waker>,
}

/// One open silence window on a consumer that currently satisfies
/// [`ConsumerState::is_stall_candidate`] (issue #414).
///
/// Progress-based, exactly like ADR-0058's connection keepalive: the window survives
/// only while no broker dispatch unit arrives. Any dispatch bumps the consumer's monotonic
/// dispatch-unit counter past the mark this window latched, which makes the next
/// [`ConsumerState::poll_stall`] tick discard the window and open a fresh one.
///
/// The keepalive watchdog cannot cover this failure mode: `PING` / `PONG` keeps flowing
/// on a connection whose broker-side dispatcher has stopped serving ONE subscription, so
/// `last_activity` never ages and no connection-level deadline ever fires (issue #414's
/// production symptom — survivors receive ~20 messages, then silence, with the broker's
/// `availablePermits` hugely negative and no client-visible error).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StallWatch {
    /// [`ConsumerState::dispatch_units_received`] as of the tick that opened this window.
    /// A later tick observing a different value has seen progress and restarts the window.
    pub(crate) mark: u64,
    /// Injected-clock instant the window opened (never `Instant::now()` — ADR-0011).
    /// The stall fires once `now >= since + consumer_stall_timeout`.
    pub(crate) since: std::time::Instant,
    /// Whether this window has already surfaced its
    /// [`crate::event::ConnectionEvent::ConsumerStalled`]. Latched so one stall episode
    /// emits exactly one event however many ticks run past the deadline; a dispatch (new
    /// `mark`) or a state change that ends candidacy replaces the window and re-arms.
    pub(crate) reported: bool,
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
    /// Current number of live [`ConsumerState::batch_ack_tracker`] entries (PIP-54
    /// per-batch ack bitsets). Magnetar-specific gauge (no Java counterpart): under a
    /// correctly-pruning ack path this is bounded by the un-acked window, so a
    /// monotonically growing value is the signature of the issue-#326 leak (cumulative
    /// acks failing to prune the entries they cover).
    pub pending_batch_acks: usize,
}

impl ConsumerStats {
    /// Fold cumulative stats snapshots from multiple child consumers (e.g.
    /// `MultiTopicsConsumer` / `PartitionedConsumer` fan-in) into one
    /// aggregate `ConsumerStats` (issue #347). Java has no equivalent
    /// aggregate type — `PartitionedConsumerImpl` doesn't expose a stats
    /// getter at all — so this is a magnetar-specific convenience, but each
    /// field's math mirrors the Java semantics for the underlying counter.
    ///
    /// Each `(ConsumerStats, Option<Histogram<u64>>)` pair is one child's
    /// [`ConsumerState::stats`] snapshot plus (ideally taken at the same
    /// instant) its [`ConsumerState::receive_latency_histogram`] clone —
    /// the histogram is required separately because `ConsumerStats` only
    /// carries the three pre-computed percentiles, not the distribution a
    /// sound merge needs.
    ///
    /// Per-field aggregation rule — each is applied inside an **exhaustive**
    /// destructure of the child `ConsumerStats` below, so adding a field to
    /// this struct is a compile error here until this fold picks a rule for
    /// it:
    /// - the six cumulative totals + `pending_batch_acks` — **saturating sum** (never wrap into
    ///   overflow noise).
    /// - `msgs_per_sec` / `bytes_per_sec` — **f64 sum** (the aggregate throughput observed across
    ///   every child is the sum of the per-child rates — fan-in, not an average).
    /// - `receive_latency_max_ms` — **exact max** across children; this is a plain stats-field
    ///   rule, independent of whether a histogram was supplied.
    /// - `receive_latency_p50_ms` / `receive_latency_p99_ms` — **recomputed from the merged
    ///   histogram** (`hdrhistogram::Histogram::add` over every supplied child histogram, then
    ///   re-queried at the same quantiles). Percentiles do not compose under per-child summing or
    ///   maxing — merging the underlying distributions is the only statistically sound way to
    ///   compute the aggregate percentile. `0` when no child supplied a histogram.
    #[must_use]
    pub fn fold(
        children: impl IntoIterator<Item = (ConsumerStats, Option<hdrhistogram::Histogram<u64>>)>,
    ) -> ConsumerStats {
        let mut agg = ConsumerStats::default();
        let mut merged_hist: Option<hdrhistogram::Histogram<u64>> = None;

        for (stats, hist) in children {
            // Exhaustive destructure: a future `ConsumerStats` field
            // addition is a compile error here until this fold picks an
            // aggregation rule for it.
            let ConsumerStats {
                total_msgs_received,
                total_bytes_received,
                total_acks_sent,
                total_acks_failed,
                total_msgs_dead_lettered,
                total_chunked_msgs_received,
                // Recomputed below from the merged histogram — a child's own
                // percentile fields are not composable and must be ignored.
                receive_latency_p50_ms: _,
                receive_latency_p99_ms: _,
                receive_latency_max_ms,
                msgs_per_sec,
                bytes_per_sec,
                pending_batch_acks,
            } = stats;

            agg.total_msgs_received = agg.total_msgs_received.saturating_add(total_msgs_received);
            agg.total_bytes_received = agg
                .total_bytes_received
                .saturating_add(total_bytes_received);
            agg.total_acks_sent = agg.total_acks_sent.saturating_add(total_acks_sent);
            agg.total_acks_failed = agg.total_acks_failed.saturating_add(total_acks_failed);
            agg.total_msgs_dead_lettered = agg
                .total_msgs_dead_lettered
                .saturating_add(total_msgs_dead_lettered);
            agg.total_chunked_msgs_received = agg
                .total_chunked_msgs_received
                .saturating_add(total_chunked_msgs_received);
            agg.pending_batch_acks = agg.pending_batch_acks.saturating_add(pending_batch_acks);
            agg.msgs_per_sec += msgs_per_sec;
            agg.bytes_per_sec += bytes_per_sec;
            agg.receive_latency_max_ms = agg.receive_latency_max_ms.max(receive_latency_max_ms);

            if let Some(h) = hist {
                match merged_hist.as_mut() {
                    Some(merged) => {
                        if let Err(err) = merged.add(&h) {
                            tracing::warn!(
                                error = %err,
                                "ConsumerStats::fold: hdrhistogram merge rejected a child \
                                 histogram (auto-resize should make this unreachable); \
                                 dropping its contribution to the merged percentiles"
                            );
                        }
                    }
                    None => merged_hist = Some(h),
                }
            }
        }

        if let Some(h) = merged_hist.as_ref().filter(|h| !h.is_empty()) {
            agg.receive_latency_p50_ms = h.value_at_quantile(0.50);
            agg.receive_latency_p99_ms = h.value_at_quantile(0.99);
        }

        agg
    }
}

/// Default for [`ConsumerState::max_pending_chunked_message`]. Mirrors Java
/// `ConsumerConfigurationData#maxPendingChunkedMessage = 10`.
pub const DEFAULT_MAX_PENDING_CHUNKED_MESSAGE: usize = 10;

/// Bound on [`ConsumerState::active_changes`] (issue #348). A Failover
/// promotion/demotion is a rare, broker-scheduled event — this cap exists
/// only to bound memory against a pathological/hostile broker flooding
/// `CommandActiveConsumerChange`, not because legitimate traffic approaches
/// it. Overflow drops the oldest entry (matches the ring semantics
/// documented on [`ConsumerState::active_changes`]).
pub const ACTIVE_CHANGES_CAP: usize = 32;

/// Default for [`ConsumerState::expire_time_of_incomplete_chunked_message`].
/// Mirrors Java `expireTimeOfIncompleteChunkedMessageMillis = 60_000` (1 minute).
pub const DEFAULT_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE: std::time::Duration =
    std::time::Duration::from_mins(1);

/// Depth-axis hard cap on a single chunked message's advertised total chunk
/// count (`num_chunks_from_msg`). A hostile/buggy broker can advertise a `total`
/// up to `i32::MAX` and stream distinct chunk_ids into ONE buffer, which the
/// breadth cap (`max_pending_chunked_message`, a count of *buffers*) does not
/// constrain. Java leaves `total` unbounded but pre-sizes a `ByteBuf` to
/// `totalChunkMsgSize`, which the broker's max-message-size bounds in practice;
/// magnetar grows a `BytesMut` lazily, so we bound the chunk count directly.
/// `10_000` is far above any legitimate message — at Pulsar's default 5 MiB
/// per-chunk wire limit that is ~48 GiB reassembled — while still rejecting the
/// `i32::MAX` attacker before the `chunk_payloads` map or the `0..total`
/// reassembly loop can blow up. Not user-configurable (Java exposes no knob
/// either); it is a pure safety floor.
pub const MAX_CHUNK_TOTAL: i32 = 10_000;

/// Depth-axis hard cap on the AGGREGATE buffered chunk-payload bytes across
/// EVERY incomplete chunked message on the consumer (`chunk_buffered_bytes`).
/// This is the tight memory ceiling: total chunk-reassembly memory can never
/// exceed `MAX_BUFFERED_CHUNK_BYTES` regardless of how the bytes are split
/// across uuids or chunk_ids, versus today's unbounded growth. A chunk that
/// would push the aggregate past this ceiling is dropped. `128 MiB` comfortably
/// admits the largest realistic chunked payloads (Pulsar's `maxMessageSize`
/// chunking targets tens of MiB) while capping both a distinct-chunk_id flood
/// within one uuid and fan-out across uuids. Not user-configurable — a pure
/// safety floor.
pub const MAX_BUFFERED_CHUNK_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug)]
struct ChunkBuffer {
    expected_chunks: i32,
    received_chunks: i32,
    /// Injected-clock timestamp of the FIRST chunk's arrival (`deliver`'s `now`
    /// parameter, never `Instant::now()` — ADR-0011). Drives the expiry sweep
    /// in [`crate::Connection::handle_timeout`]; the earliest `received_at`
    /// across all buffers is surfaced through
    /// [`crate::Connection::poll_timeout`] so the driver schedules the wake.
    received_at: std::time::Instant,
    /// Partial payload accumulator. Chunks may arrive in order; out-of-order chunk arrival is
    /// not expected over a single connection (the broker dispatches in order), but if it does,
    /// the buffer is indexed by `chunk_id` to make reassembly robust.
    chunk_payloads: HashMap<i32, Bytes>,
    /// Arc-wrapped first-chunk metadata so the reassembled `IncomingMessage`
    /// hands the consumer an `Arc` clone instead of a deep copy on final
    /// assembly. Stored as `Arc` from the first-chunk arrival so the
    /// pending-chunk path never deep-clones the metadata either.
    first_metadata: std::sync::Arc<pb::MessageMetadata>,
    first_chunk_message_id: Option<MessageId>,
    broker_entry_metadata: Option<std::sync::Arc<pb::BrokerEntryMetadata>>,
    redelivery_count: u32,
}

impl ChunkBuffer {
    /// Total buffered payload bytes across every chunk this buffer holds. Used
    /// by [`ConsumerState`] to keep its aggregate `chunk_buffered_bytes`
    /// accounting in lock-step when the buffer is removed (reassembled, evicted,
    /// or expired).
    fn buffered_bytes(&self) -> usize {
        self.chunk_payloads
            .values()
            .map(bytes::Bytes::len)
            .fold(0usize, usize::saturating_add)
    }
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
    /// Construct a new consumer with the default [`crate::receiver_queue::Fixed`]
    /// policy of `receiver_queue_size`. Behaviour is identical to the pre-#301
    /// raw-`usize` design.
    pub fn new(
        handle: ConsumerHandle,
        topic: String,
        subscription: String,
        receiver_queue_size: usize,
    ) -> Self {
        Self::with_policy(
            handle,
            topic,
            subscription,
            crate::receiver_queue::fixed(receiver_queue_size),
            None,
        )
    }

    /// Construct a new consumer with an explicit receiver-queue [`policy`]
    /// (issue #301). The initial target is seeded from `policy.initial()`.
    /// `adjust_interval` is the spacing between auto-adjust ticks; `None`
    /// disables auto-adjust (the right choice for a
    /// [`crate::receiver_queue::Fixed`] policy, whose target never moves).
    ///
    /// [`policy`]: Self::policy
    pub fn with_policy(
        handle: ConsumerHandle,
        topic: String,
        subscription: String,
        policy: std::sync::Arc<dyn crate::receiver_queue::ReceiverQueuePolicy>,
        adjust_interval: Option<std::time::Duration>,
    ) -> Self {
        let receiver_queue_size = policy.initial();
        Self {
            handle,
            topic,
            subscription,
            consumer_name: None,
            receiver_queue_size,
            policy,
            queued_bytes: 0,
            adjust_interval,
            last_adjust_at: None,
            granted_permits: 0,
            permit_balance: 0,
            dispatch_units_received: 0,
            stall_watch: None,
            stall_recovery_attempts: 0,
            consumed_since_flow: 0,
            queue: VecDeque::new(),
            chunk_reassembly: HashMap::new(),
            chunk_reassembly_order: VecDeque::new(),
            chunk_buffered_bytes: 0,
            max_pending_chunked_message: DEFAULT_MAX_PENDING_CHUNKED_MESSAGE,
            auto_ack_oldest_chunked_message_on_queue_full: false,
            expire_time_of_incomplete_chunked_message: Some(
                DEFAULT_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE,
            ),
            chunk_auto_ack_pending: Vec::new(),
            pending_seek: None,
            receive_wakers: Slab::new(),
            closed: false,
            unsubscribe_request_id: None,
            has_ever_attached: false,
            transient_subscribe_attempts: 0,
            last_subscribe_error: None,
            terminal_failure: None,
            subscribe_waiter_id: None,
            subscribe_waiter_request: None,
            subscribe_waiter_completed: false,
            flow_on_subscribe_ack: false,
            flow_on_subscribe_ack_request: None,
            initial_grant_due: false,
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
            batch_ack_tracker: rustc_hash::FxHashMap::default(),
            ack_tracker: None,
            crypto_failure_action: crate::conn::CryptoFailureAction::Fail,
            // 3 significant digits, auto-resizing — same precision the Java client uses for
            // its ConsumerStatsRecorder. See [`crate::producer::new_latency_histogram`] for
            // the invariant-#6 (no panics) safety chain.
            receive_latency_hist: crate::producer::new_latency_histogram(),
            last_rate_snapshot: None,
            current_msgs_per_sec: 0.0,
            current_bytes_per_sec: 0.0,
            shadow_metadata: None,
            is_active: None,
            active_changes: VecDeque::new(),
            active_change_wakers: Slab::new(),
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
    ///
    /// Sampling is **connection-driven when
    /// [`ConnectionConfig::stats_interval`](crate::conn::ConnectionConfig) is
    /// set**: `Connection::handle_timeout` re-samples every registered consumer
    /// once per interval, off a deadline `Connection::poll_timeout` arms from
    /// this slot's own [`Self::last_rate_snapshot`] (ADR-0089). That is
    /// magnetar's equivalent of Java's per-recorder tick on the client-wide
    /// `HashedWheelTimer`.
    ///
    /// The knob defaults to `Some(60 s)` (Java parity). Setting it to `None`
    /// leaves sampling **caller-driven**: no engine calls this, so a caller that
    /// never invokes it — and disabled the sweep — leaves
    /// [`Self::current_msgs_per_sec`] / [`Self::current_bytes_per_sec`] at `0.0`
    /// forever. Calling it directly
    /// while the sweep is also running is supported but re-seeds the window,
    /// so the two cadences interleave; pick one.
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
            pending_batch_acks: self.batch_ack_tracker.len(),
        }
    }

    /// 50th percentile receive latency, in milliseconds. Mirrors Java
    /// `ConsumerStatsRecorder#getRcvLatencyMillis50pct`. Returns 0 when the histogram
    /// is absent (constructor failure, statically impossible) or empty.
    #[must_use]
    pub fn receive_latency_p50_ms(&self) -> u64 {
        let Some(h) = self.receive_latency_hist.as_ref() else {
            return 0;
        };
        if h.is_empty() {
            return 0;
        }
        h.value_at_quantile(0.50)
    }

    /// 99th percentile receive latency, in milliseconds. Mirrors Java
    /// `ConsumerStatsRecorder#getRcvLatencyMillis99pct`. Returns 0 when the histogram
    /// is absent (constructor failure, statically impossible) or empty.
    #[must_use]
    pub fn receive_latency_p99_ms(&self) -> u64 {
        let Some(h) = self.receive_latency_hist.as_ref() else {
            return 0;
        };
        if h.is_empty() {
            return 0;
        }
        h.value_at_quantile(0.99)
    }

    /// Maximum observed receive latency, in milliseconds. Mirrors Java
    /// `ConsumerStatsRecorder#getRcvLatencyMillisMax`. Returns 0 when the histogram
    /// is absent (constructor failure, statically impossible) or empty.
    #[must_use]
    pub fn receive_latency_max_ms(&self) -> u64 {
        let Some(h) = self.receive_latency_hist.as_ref() else {
            return 0;
        };
        if h.is_empty() {
            return 0;
        }
        h.max()
    }

    /// Clone of the live receive-latency histogram (issue #347). For callers
    /// that need the raw distribution rather than the three pre-computed
    /// percentiles [`Self::stats`] exposes — primarily
    /// [`ConsumerStats::fold`], which merges several consumers' histograms
    /// via `hdrhistogram::Histogram::add` to compute a statistically sound
    /// aggregate percentile (percentiles do not compose under per-child
    /// summing or maxing, only a real histogram merge is). `None` when the
    /// histogram was never initialised (constructor failure, statically
    /// impossible — invariant #6).
    #[must_use]
    pub fn receive_latency_histogram(&self) -> Option<hdrhistogram::Histogram<u64>> {
        self.receive_latency_hist.clone()
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
        self.granted_permits = self.granted_permits.saturating_add(permits);
        self.permit_balance = self.permit_balance.saturating_add(permits);
        self.clear_stall_watch();
        Some(pb::CommandFlow {
            consumer_id: self.handle.0,
            message_permits: permits,
        })
    }

    fn record_broker_permit_consumed(&mut self) {
        self.consumed_since_flow = self.consumed_since_flow.saturating_add(1);
    }

    /// Account for exactly ONE broker dispatch unit arriving on this consumer.
    ///
    /// Issue #349 owns the first half — decrement the REAL
    /// [`Self::permit_balance`] once per unit the broker actually spent a permit on.
    /// Issue #414 owns the second — bump the monotonic
    /// [`Self::dispatch_units_received`] progress mark the stall watchdog reads.
    /// They are one call precisely so a future dispatch site cannot update one and
    /// forget the other: a missed balance decrement mis-reports starvation, a missed
    /// progress bump makes the watchdog cry stall on a perfectly healthy consumer.
    ///
    /// ADR-0103 hangs a third off the same call for the same reason: one dispatch unit
    /// arriving is the ONLY definition of progress that clears
    /// [`Self::stall_recovery_attempts`], so the automatic-recovery budget is restored
    /// exactly when — and only when — the broker demonstrably started dispatching again.
    /// A re-subscribe that the broker acked but never dispatched against is not progress
    /// and must not refund the attempt that bought it.
    ///
    /// Carries no clock (ADR-0011): the mark is a counter, so the three call sites keep
    /// their existing signatures.
    fn record_dispatch_unit(&mut self) {
        self.permit_balance = self.permit_balance.saturating_sub(1);
        self.dispatch_units_received = self.dispatch_units_received.saturating_add(1);
        self.stall_recovery_attempts = 0;
    }

    /// Drop any open stall window (issue #414). Called at every permit-grant site and by
    /// [`crate::Connection::resubscribe_consumer_in_place`]: a fresh grant, or a fresh
    /// broker-side dispatcher slot, restarts the silence window from the next tick rather
    /// than inheriting a start instant that predates it.
    pub(crate) fn clear_stall_watch(&mut self) {
        self.stall_watch = None;
    }

    /// Open the stall window at `now` (issue #414), discarding any older one.
    ///
    /// Called by [`crate::Connection::initial_flow`], the one grant site that is handed an
    /// injected clock (ADR-0011) — subscribe ack, reconnect rebuild, post-seek resubscribe,
    /// the #307 Failover re-arm, and the #414 caller-driven recovery all route through it.
    /// Seeding here rather than waiting for the first sweep is what makes detection take
    /// `consumer_stall_timeout` instead of `consumer_stall_timeout + keepalive_interval`:
    /// `poll_timeout` has no instant to arm from until a window exists, and on an otherwise
    /// idle connection the keepalive deadline is the only thing that would produce that
    /// first sweep. A full grant is also exactly the moment a #414 wedge begins — the
    /// broker acknowledged permits it will never spend — so it is the right zero.
    ///
    /// Unconditional: a fresh grant is a fresh promise and always deserves a fresh window.
    /// The other grant sites ([`Self::maybe_flow`], [`Self::adjust_receiver_queue`]) carry
    /// no clock and only [`Self::clear_stall_watch`], which the next sweep re-seeds.
    pub(crate) fn arm_stall_watch(&mut self, now: std::time::Instant) {
        self.stall_watch = Some(StallWatch {
            mark: self.dispatch_units_received,
            since: now,
            reported: false,
        });
    }

    /// `true` while this consumer is in the state issue #414 wedges in: the broker has
    /// been granted permits it has not spent (`permit_balance > 0`), the local queue is
    /// empty so nothing is waiting on the user, and the consumer is dispatch-eligible.
    ///
    /// The eligibility set is deliberately the SAME one the #307 Failover re-arm gate uses
    /// (`ActiveConsumerChange` arm in [`crate::Connection`]): not closed, not paused, no
    /// in-flight seek freezing the queue, not terminally failed, not end-of-topic, and not
    /// mid-re-attach. Each of those explains the silence without a broker fault, and a
    /// watchdog that fired on them would be reporting the user's own gating back at them.
    ///
    /// Pure read — no clock, no mutation.
    #[must_use]
    pub fn is_stall_candidate(&self) -> bool {
        self.permit_balance > 0
            && self.queue.is_empty()
            && !self.closed
            && !self.paused
            && !self.reached_end_of_topic
            && self.pending_seek.is_none()
            && self.terminal_failure.is_none()
            && !self.flow_on_subscribe_ack
    }

    /// Deadline at which an open, not-yet-reported stall window becomes due, or `None`
    /// when the watchdog is disabled, no window is open, or this window already fired.
    ///
    /// Surfaced through [`crate::Connection::poll_timeout`] so the driver schedules a
    /// deterministic wake — without it the sweep would only fire opportunistically on an
    /// unrelated deadline, which is seed-divergent under the moonpool engine (the same
    /// rationale [`Self::next_adjust_deadline`] and `next_chunk_expiry_deadline` carry).
    ///
    /// A candidate with no window yet arms nothing here: there is no instant to arm from.
    /// [`crate::Connection::initial_flow`] seeds one at grant time, so in practice a
    /// granted consumer is armed from the moment the broker was told it may dispatch. A consumer
    /// that regains candidacy some other way — its queue draining empty after `maybe_flow`
    /// topped the grant up — waits for the next sweep to seed it, and the keepalive deadline
    /// guarantees one (ADR-0058).
    #[must_use]
    pub fn next_stall_deadline(&self, window: std::time::Duration) -> Option<std::time::Instant> {
        let watch = self.stall_watch.as_ref()?;
        if watch.reported {
            return None;
        }
        Some(crate::time::deadline_with_clamp(watch.since, window))
    }

    /// Run one stall-watchdog tick against the injected `now` (ADR-0011).
    ///
    /// Returns `Some(silent_for)` exactly once per stall episode — on the first tick at or
    /// after `window` has elapsed with no dispatch unit — so the caller can surface one
    /// [`crate::event::ConnectionEvent::ConsumerStalled`]. Every later tick in the same
    /// episode returns `None`.
    ///
    /// Emitting the event is the only effect **this method** has, and the only effect the
    /// watchdog has at all while `consumer_stall_auto_recovery` is unset: recovery is then
    /// explicit ([`crate::Connection::resubscribe_consumer_in_place`], or an operator-side
    /// `topics unload`). ADR-0101 rejected an unconditional automatic re-subscribe because
    /// a broker hiccup would become a re-subscribe storm across every partition at once
    /// and would hide the broker-side defect issue #414 is actually about; ADR-0103 admits
    /// it only opt-in and only bounded, driven by
    /// [`crate::Connection::handle_timeout`] from the `Some` this returns — one attempt
    /// per episode, at most `consumer_stall_auto_recovery` attempts per stall streak, and
    /// the event is emitted either way so the defect is still reported rather than
    /// papered over.
    pub fn poll_stall(
        &mut self,
        window: std::time::Duration,
        now: std::time::Instant,
    ) -> Option<std::time::Duration> {
        if !self.is_stall_candidate() {
            // Progress, user gating, or a terminal transition ended candidacy — drop the
            // window so the next candidacy opens a fresh one (this is what re-arms the
            // once-per-episode latch).
            self.stall_watch = None;
            return None;
        }
        let mark = self.dispatch_units_received;
        match self.stall_watch {
            // Same silence window, still running.
            Some(watch) if watch.mark == mark => {
                if watch.reported || now < crate::time::deadline_with_clamp(watch.since, window) {
                    return None;
                }
                self.stall_watch = Some(StallWatch {
                    reported: true,
                    ..watch
                });
                Some(now.saturating_duration_since(watch.since))
            }
            // No window yet, or a dispatch landed since the last tick: (re-)open one.
            _ => {
                self.stall_watch = Some(StallWatch {
                    mark,
                    since: now,
                    reported: false,
                });
                None
            }
        }
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
        self.record_broker_permit_consumed();
        // Issue #349: a marker is one broker-dispatched unit too — decrement the
        // REAL balance directly (not through `record_broker_permit_consumed`,
        // which only tracks the pop-driven `consumed_since_flow` counter, the
        // wrong site for the live balance). Issue #414: the same call bumps the
        // watchdog's progress mark, so a marker-only stream (a replicated
        // subscription with no user traffic) counts as broker liveness.
        self.record_dispatch_unit();
    }

    /// Force an initial flow for the current receiver-queue target.
    ///
    /// Issue #301: `receiver_queue_size` is the policy's *current* target (seeded
    /// from `policy.initial()` at subscribe time, ramped by
    /// [`Self::adjust_receiver_queue`]). All three flow-emission sites in the
    /// connection — fresh subscribe ack, reconnect re-attach, and the #307
    /// Failover active-consumer-change re-arm — route through here, so each
    /// grants the policy's CURRENT target rather than a stale raw value.
    pub fn initial_flow(&mut self) -> pb::CommandFlow {
        let permits = self.receiver_queue_size as u32;
        self.granted_permits = permits;
        self.permit_balance = permits;
        self.consumed_since_flow = 0;
        // Issue #427: this attach's initial grant is now on its way to the broker, so no
        // other grant site owes it one. `Connection::initial_flow` reads the flag back to
        // stay idempotent per attach.
        self.initial_grant_due = false;
        // Issue #414: a fresh full grant (subscribe ack, reconnect rebuild, post-seek
        // resubscribe, #307 re-arm, #414 caller-driven resubscribe) restarts the stall
        // window. Keeping the old window would let a consumer that was already silent
        // fire a stall the instant it is re-armed, before the broker had any chance to
        // dispatch against the new grant.
        self.clear_stall_watch();
        pb::CommandFlow {
            consumer_id: self.handle.0,
            message_permits: permits,
        }
    }

    /// Build the [`crate::receiver_queue::FlowStats`] snapshot for this
    /// consumer (issue #301). Pure read of the current state under the per-slot
    /// lock — carries no clock. `partitions` is supplied by the connection /
    /// façade aggregate (a per-partition `ConsumerState` does not itself know the
    /// partition count); `1` for a non-partitioned consumer.
    #[must_use]
    pub fn flow_stats(&self, partitions: usize) -> crate::receiver_queue::FlowStats {
        let avg_message_bytes = self
            .total_bytes_received
            .checked_div(self.total_msgs_received)
            .unwrap_or(0);
        crate::receiver_queue::FlowStats {
            current_queue_size: self.receiver_queue_size,
            queued_messages: self.queue.len(),
            // Issue #349: feed the REAL decrementing balance, not the
            // purely-additive `granted_permits` mirror, so `0` here is a
            // genuine starvation signal (see `Self::permit_balance`'s doc).
            available_permits: self.permit_balance,
            consume_rate_msgs_per_s: self.current_msgs_per_sec,
            avg_message_bytes,
            in_flight_bytes: self.queued_bytes,
            partitions,
        }
    }

    /// Run one auto-adjust tick (issue #301). Recomputes the receiver-queue
    /// target via `policy.adjust(&FlowStats)` and reconciles the broker grant:
    ///
    /// - **Grow** (`new > current`): the broker has fewer permits than the new target wants, so
    ///   emit an incremental `CommandFlow` for the delta and bump both `granted_permits` and
    ///   `permit_balance`. This is what keeps a starving consumer fed.
    /// - **Shrink / hold** (`new <= current`): permits already granted to the broker cannot be
    ///   un-granted, so emit nothing — the surplus drains naturally as messages arrive and
    ///   `maybe_flow` simply asks for less next time (its threshold is `new / 2`).
    ///
    /// Records `now` as the last-adjust timestamp so [`Self::next_adjust_deadline`]
    /// schedules the following tick. Pure given (`policy`, `FlowStats`, `now`):
    /// no clock is read inside — `now` is injected by
    /// [`crate::Connection::handle_timeout`] (ADR-0011). Frozen states (closed,
    /// in-flight seek, paused, terminal) are skipped so the policy never fights
    /// the user's explicit gating.
    ///
    /// Returns `Some(CommandFlow)` when the target grew, `None` otherwise.
    pub fn adjust_receiver_queue(
        &mut self,
        now: std::time::Instant,
        partitions: usize,
    ) -> Option<pb::CommandFlow> {
        self.last_adjust_at = Some(now);
        if self.closed
            || self.pending_seek.is_some()
            || self.paused
            || self.terminal_failure.is_some()
        {
            return None;
        }
        // Issue #349 churn-window guard: `granted_permits == 0` only occurs
        // right after a reset / terminal-failure / same-broker
        // `CloseConsumer` zeroing — there is no outstanding grant for the
        // broker to have dispatched against, so a zero `permit_balance` here
        // reflects the churn window, not load starvation. Skip the tick
        // entirely rather than let the policy misread it and grow (or emit a
        // flow the broker would drop against a torn-down consumer id).
        if self.granted_permits == 0 {
            return None;
        }
        let stats = self.flow_stats(partitions);
        let new_target = self.policy.adjust(&stats).max(1);
        let current = self.receiver_queue_size;
        self.receiver_queue_size = new_target;
        if new_target <= current {
            // Shrink or hold: cannot un-grant permits, so let them drain. The
            // smaller `maybe_flow` threshold (`new_target / 2`) then asks for
            // less on the next refill.
            return None;
        }
        // Grow: top the broker's grant up to the new target with an incremental
        // flow for the delta the broker does not yet have.
        let want = new_target as u32;
        let have = self.granted_permits;
        let delta = want.saturating_sub(have);
        if delta == 0 {
            return None;
        }
        self.granted_permits = self.granted_permits.saturating_add(delta);
        self.permit_balance = self.permit_balance.saturating_add(delta);
        self.clear_stall_watch();
        Some(pb::CommandFlow {
            consumer_id: self.handle.0,
            message_permits: delta,
        })
    }

    /// Earliest moment at which the next auto-adjust tick is due, or `None` when
    /// auto-adjust is disabled (`adjust_interval == None`, the default for the
    /// [`crate::receiver_queue::Fixed`] policy). Surfaced through
    /// [`crate::Connection::poll_timeout`] so the driver schedules a
    /// deterministic wake for [`crate::Connection::handle_timeout`]'s adjust
    /// sweep — without it the tick would only fire opportunistically on an
    /// unrelated deadline, which is seed-divergent under the moonpool engine.
    #[must_use]
    pub fn next_adjust_deadline(&self) -> Option<std::time::Instant> {
        let interval = self.adjust_interval?;
        if self.closed || self.terminal_failure.is_some() {
            return None;
        }
        // `None` (no tick yet) yields `None`; the driver re-derives the deadline
        // from `poll_timeout` once `arm_adjust_clock` sets `last_adjust_at`.
        self.last_adjust_at
            .map(|last| crate::time::deadline_with_clamp(last, interval))
    }

    /// Seed the first adjust deadline at `now + adjust_interval` so the
    /// [`Self::next_adjust_deadline`] schedule has a starting point.
    ///
    /// Called from [`crate::Connection::initial_flow`] at subscribe-ack time —
    /// the schedule's dedicated bootstrap, which is what keeps the first tick
    /// independent of whichever unrelated deadline happens to fire first
    /// (`Connection::handle_timeout` keeps a backstop call for a consumer that
    /// somehow ticks without ever having been flowed).
    ///
    /// Idempotent and no-op when auto-adjust is disabled: it only fires while
    /// `last_adjust_at` is `None` and `adjust_interval` is `Some`, so the
    /// re-attach / Failover-promotion re-flows that also route through
    /// `initial_flow` neither restart nor skew a running schedule.
    pub fn arm_adjust_clock(&mut self, now: std::time::Instant) {
        if self.adjust_interval.is_some() && self.last_adjust_at.is_none() {
            self.last_adjust_at = Some(now);
        }
    }

    /// Pop the next available message for the user. Caller wakes its future when a new message
    /// is delivered (the [`Connection`](crate::Connection) does this automatically).
    ///
    /// Records the receive latency — `now - msg.arrived_at`, both ends of the subtraction being
    /// engine-injected instants ([ADR-0011], [ADR-0086]) — into [`Self::receive_latency_hist`]
    /// so [`ConsumerStats`] can surface p50/p99/max. The state machine never reads the host
    /// clock, so under the moonpool engine the recorded sample is a pure function of the
    /// virtual clock and reproduces bit-for-bit for a given seed.
    ///
    /// [ADR-0011]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0011-clock-injection-sans-io.md
    /// [ADR-0086]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0086-inject-now-into-proto-latency-recording.md
    pub fn pop_message(&mut self, now: std::time::Instant) -> Option<IncomingMessage> {
        let msg = self.queue.pop_front()?;
        // Issue #301: keep the buffered-queue-bytes counter in lock-step with
        // the enqueue bump in `classify_and_queue`.
        self.queued_bytes = self.queued_bytes.saturating_sub(msg.payload.len() as u64);
        self.record_broker_permit_consumed();
        if let Some(h) = self.receive_latency_hist.as_mut() {
            // `saturating_duration_since`, never `-`: `Instant - Instant` panics on underflow
            // and invariant #6 forbids panics outside `#[cfg(test)]`. A `now` behind
            // `arrived_at` (a caller reusing a stale snapshot, or a virtual clock rewound
            // across a reset) records 0 ms instead of aborting the process.
            let latency_ms =
                u64::try_from(now.saturating_duration_since(msg.arrived_at).as_millis())
                    .unwrap_or(u64::MAX);
            h.saturating_record(latency_ms);
        }
        Some(msg)
    }

    /// Number of messages waiting to be popped.
    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    /// Number of distinct incomplete chunked messages currently buffered.
    /// Test-only visibility into the bounded reassembly state.
    #[cfg(test)]
    fn pending_chunk_count(&self) -> usize {
        self.chunk_reassembly.len()
    }

    /// Remove a uuid from the FIFO insertion-order index. Linear in the queue
    /// length, which is bounded by `max_pending_chunked_message` (default 10),
    /// so this is effectively O(1). Keeps the index in lock-step with
    /// `chunk_reassembly` on every removal (reassembly, eviction, expiry).
    fn forget_chunk_order(&mut self, uuid: &str) {
        if let Some(pos) = self.chunk_reassembly_order.iter().position(|u| u == uuid) {
            self.chunk_reassembly_order.remove(pos);
        }
    }

    /// Evict the OLDEST incomplete chunked message when the breadth cap is
    /// breached. Mirrors Java `ConsumerImpl#removeOldestPendingChunkedMessage`
    /// → `removeChunkMessage`:
    ///
    /// - `auto_ack_oldest_chunked_message_on_queue_full = true`: stage the partial's first-chunk id
    ///   for an individual `CommandAck` (the broker treats it as consumed) before dropping the
    ///   buffer.
    /// - `false` (the default): drop the buffer WITHOUT acking, so the broker eventually redelivers
    ///   the whole message.
    ///
    /// Removes the uuid from BOTH `chunk_reassembly` AND the FIFO order index
    /// atomically and decrements the aggregate byte accounting.
    fn remove_oldest_pending_chunked_message(&mut self) {
        // Skip any stale front entries whose buffer was already removed (the
        // index can briefly outlive a map entry in pathological replays).
        while let Some(uuid) = self.chunk_reassembly_order.pop_front() {
            let Some(entry) = self.chunk_reassembly.remove(&uuid) else {
                continue;
            };
            self.chunk_buffered_bytes = self
                .chunk_buffered_bytes
                .saturating_sub(entry.buffered_bytes());
            if self.auto_ack_oldest_chunked_message_on_queue_full {
                if let Some(id) = entry.first_chunk_message_id {
                    self.chunk_auto_ack_pending.push(id);
                }
            }
            tracing::warn!(
                target: "magnetar_proto::consumer",
                consumer_id = self.handle.0,
                received_chunks = entry.received_chunks,
                total_chunks = entry.expected_chunks,
                auto_ack = self.auto_ack_oldest_chunked_message_on_queue_full,
                "evicted oldest incomplete chunked message (max_pending_chunked_message breach)",
            );
            return;
        }
    }

    /// Earliest moment at which an incomplete chunked message becomes eligible
    /// for the expiry sweep, or `None` when expiry is disabled or no buffer is
    /// pending. Surfaced through [`crate::Connection::poll_timeout`] so the
    /// driver schedules a deterministic wake; without this the sweep would only
    /// fire opportunistically on an unrelated tick (seed-divergent under the
    /// moonpool engine). The front of `chunk_reassembly_order` is the oldest
    /// buffer, so its `received_at + expire_time` is the nearest deadline.
    pub fn next_chunk_expiry_deadline(&self) -> Option<std::time::Instant> {
        let expire = self.expire_time_of_incomplete_chunked_message?;
        let oldest = self.chunk_reassembly_order.front()?;
        let entry = self.chunk_reassembly.get(oldest)?;
        Some(crate::time::deadline_with_clamp(entry.received_at, expire))
    }

    /// Sweep every incomplete chunked message older than
    /// `expire_time_of_incomplete_chunked_message` relative to the injected
    /// `now`. Fully removes each expired buffer from BOTH `chunk_reassembly`
    /// AND the FIFO order index and decrements the aggregate byte accounting.
    /// Mirrors Java `ConsumerImpl#removeExpireIncompleteChunkedMessages`, which
    /// acks expired partials unconditionally (`removeChunkMessage(.., true)`);
    /// we stage the first-chunk id for ack only when
    /// `auto_ack_oldest_chunked_message_on_queue_full` is set, keeping the
    /// default broker-redelivers semantics consistent with eviction.
    /// No-op when expiry is disabled.
    pub fn sweep_expired_chunks(&mut self, now: std::time::Instant) {
        let Some(expire) = self.expire_time_of_incomplete_chunked_message else {
            return;
        };
        // The order index is sorted oldest-first, so stop at the first
        // not-yet-expired buffer (every later one is younger).
        while let Some(uuid) = self.chunk_reassembly_order.front().cloned() {
            let Some(entry) = self.chunk_reassembly.get(&uuid) else {
                // Stale index entry — drop it and continue.
                self.chunk_reassembly_order.pop_front();
                continue;
            };
            if now < crate::time::deadline_with_clamp(entry.received_at, expire) {
                break;
            }
            self.chunk_reassembly_order.pop_front();
            // Re-take by value to recover the byte accounting + ack id.
            if let Some(entry) = self.chunk_reassembly.remove(&uuid) {
                self.chunk_buffered_bytes = self
                    .chunk_buffered_bytes
                    .saturating_sub(entry.buffered_bytes());
                if self.auto_ack_oldest_chunked_message_on_queue_full {
                    if let Some(id) = entry.first_chunk_message_id {
                        self.chunk_auto_ack_pending.push(id);
                    }
                }
                tracing::warn!(
                    target: "magnetar_proto::consumer",
                    consumer_id = self.handle.0,
                    received_chunks = entry.received_chunks,
                    total_chunks = entry.expected_chunks,
                    "expired incomplete chunked message (expire_time_of_incomplete_chunked_message)",
                );
            }
        }
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
                // F1: validate `chunk_id` against `total`. A malformed broker (or replay /
                // protocol-violation scenario) could deliver a chunk with `chunk_id >= total`
                // or a negative-looking i32. Both are protocol-level violations: drop the
                // chunk and bump the metric without panicking. Mirrors the defensive
                // bounds-check the Java consumer added in `ConsumerImpl#processMessageChunk`
                // after CVE-style fuzzing of malformed chunk metadata.
                if chunk_id < 0 || chunk_id >= total {
                    tracing::warn!(
                        consumer_id = self.handle.0,
                        chunk_id,
                        total_chunks = total,
                        "drop chunk with out-of-range chunk_id (protocol violation)",
                    );
                    return Ok(DeliverOutcome::Dropped);
                }
                // Depth-axis DoS bound: a hostile broker can advertise `total`
                // up to `i32::MAX` and stream distinct chunk_ids into ONE
                // buffer, blowing it up independent of the per-buffer breadth
                // cap. Reject the chunk before it can pre-size the
                // `chunk_payloads` map or the `0..total` reassembly loop.
                if total > MAX_CHUNK_TOTAL {
                    tracing::warn!(
                        consumer_id = self.handle.0,
                        total_chunks = total,
                        max_chunk_total = MAX_CHUNK_TOTAL,
                        "drop chunk advertising an out-of-bounds total chunk count",
                    );
                    return Ok(DeliverOutcome::Dropped);
                }
                let uuid = metadata.uuid.clone().unwrap_or_default();
                // Breadth-axis bound: only a GENUINE first chunk (`chunk_id ==
                // 0`) may (re)create a buffer. A straggler non-first chunk for
                // an unknown/evicted uuid is dropped — never fabricate a
                // corrupt buffer from non-first metadata (its `first_metadata`
                // / `first_chunk_message_id` would be wrong). Mirrors Java
                // `processMessageChunk`'s `chunkId == 0` gate on buffer
                // creation; the duplicate / out-of-order paths there discard.
                if chunk_id != 0 && !self.chunk_reassembly.contains_key(&uuid) {
                    tracing::warn!(
                        consumer_id = self.handle.0,
                        chunk_id,
                        total_chunks = total,
                        uuid_present = !uuid.is_empty(),
                        "drop straggler non-first chunk for an unknown/evicted message",
                    );
                    return Ok(DeliverOutcome::Dropped);
                }
                // Arc-wrap on first-chunk arrival so the ChunkBuffer never
                // holds a deep-cloned metadata copy across the
                // pending-chunk window. Re-wrapping `broker_entry_metadata`
                // is similarly one allocation per chunked message, not per
                // chunk.
                let is_new = !self.chunk_reassembly.contains_key(&uuid);
                // Insert the buffer on a genuine first chunk; the returned
                // `&mut` is unused (we re-borrow after the breadth-cap eviction,
                // which may mutate the map) — the call is kept for its insert
                // side effect only.
                let _ = self
                    .chunk_reassembly
                    .entry(uuid.clone())
                    .or_insert_with(|| ChunkBuffer {
                        expected_chunks: total,
                        received_chunks: 0,
                        received_at: now,
                        chunk_payloads: HashMap::new(),
                        first_metadata: std::sync::Arc::new(metadata.clone()),
                        first_chunk_message_id: Some(message_id),
                        broker_entry_metadata: broker_entry_metadata
                            .clone()
                            .map(std::sync::Arc::new),
                        redelivery_count: redelivery,
                    });
                if is_new {
                    // Track FIFO insertion order so eviction + the expiry sweep
                    // can find the oldest buffer in O(1) (Java
                    // `pendingChunkedMessageUuidQueue`).
                    self.chunk_reassembly_order.push_back(uuid.clone());
                    // Breadth cap: once a NEW uuid pushes the map past the cap,
                    // evict the oldest incomplete message. `0` disables the cap
                    // (Java's `maxPendingChunkedMessage > 0` guard).
                    if self.max_pending_chunked_message > 0
                        && self.chunk_reassembly.len() > self.max_pending_chunked_message
                    {
                        self.remove_oldest_pending_chunked_message();
                    }
                    // Re-borrow: `remove_oldest_*` may have mutated the map (it
                    // never evicts the just-inserted uuid — the front is older).
                    let Some(entry) = self.chunk_reassembly.get_mut(&uuid) else {
                        return Ok(DeliverOutcome::Dropped);
                    };
                    if entry
                        .chunk_payloads
                        .insert(chunk_id, body.clone())
                        .is_none()
                    {
                        entry.received_chunks += 1;
                        self.chunk_buffered_bytes =
                            self.chunk_buffered_bytes.saturating_add(body.len());
                    }
                } else {
                    let Some(entry) = self.chunk_reassembly.get_mut(&uuid) else {
                        return Ok(DeliverOutcome::Dropped);
                    };
                    // Depth-axis bound: drop a chunk that would push the
                    // AGGREGATE buffered bytes (across every incomplete buffer)
                    // past the ceiling. Bounds the distinct-chunk_id flood
                    // within one uuid as well as fan-out across uuids.
                    if self.chunk_buffered_bytes.saturating_add(body.len())
                        > MAX_BUFFERED_CHUNK_BYTES
                    {
                        tracing::warn!(
                            consumer_id = self.handle.0,
                            chunk_id,
                            total_chunks = total,
                            buffered_bytes = self.chunk_buffered_bytes,
                            max_buffered_chunk_bytes = MAX_BUFFERED_CHUNK_BYTES,
                            "drop chunk exceeding the per-message buffered-bytes cap",
                        );
                        return Ok(DeliverOutcome::Dropped);
                    }
                    if entry
                        .chunk_payloads
                        .insert(chunk_id, body.clone())
                        .is_some()
                    {
                        // Duplicate chunk_id — drop without advancing progress
                        // and without growing the byte accounting.
                        return Ok(DeliverOutcome::Dropped);
                    }
                    entry.received_chunks += 1;
                    self.chunk_buffered_bytes =
                        self.chunk_buffered_bytes.saturating_add(body.len());
                }
                // Re-borrow immutably for the progress check + log; the buffer
                // is guaranteed present (just inserted/updated above).
                let Some(entry) = self.chunk_reassembly.get(&uuid) else {
                    return Ok(DeliverOutcome::Dropped);
                };
                // ADR-0054 §5: chunk-reassembly progress has no
                // `ConnectionEvent`, so proto logs it at the point of
                // detection. The broker-assigned chunk UUID is logged as a
                // presence boolean only (hostile-peer-controlled string).
                tracing::debug!(
                    target: "magnetar_proto::consumer",
                    consumer_id = self.handle.0,
                    chunk_id,
                    total_chunks = total,
                    received_chunks = entry.received_chunks,
                    uuid_present = !uuid.is_empty(),
                    "chunk buffered for reassembly",
                );
                if entry.received_chunks < entry.expected_chunks {
                    self.record_broker_permit_consumed();
                    // Issue #349: this chunk is one dispatch unit even though
                    // it never reaches `classify_and_queue` (reassembly is
                    // still pending) — decrement the REAL balance directly
                    // rather than through `record_broker_permit_consumed`
                    // (which only tracks the pop-driven `consumed_since_flow`
                    // counter, the wrong site for the live balance). Issue
                    // #414: a consumer mid-reassembly is receiving, so the
                    // same call bumps the watchdog's progress mark.
                    self.record_dispatch_unit();
                    return Ok(DeliverOutcome::Buffered);
                }
                // All chunks present — assemble. Take the buffer out by value
                // first so the byte accounting + FIFO order index stay in
                // lock-step with the map. Invariant #6 (no panics in production
                // code): an `if let Some` drops the chunk gracefully if a
                // concurrent mutation ever removed it (impossible today —
                // `&mut self` — but defensive).
                let Some(mut entry) = self.chunk_reassembly.remove(&uuid) else {
                    return Ok(DeliverOutcome::Dropped);
                };
                self.forget_chunk_order(&uuid);
                self.chunk_buffered_bytes = self
                    .chunk_buffered_bytes
                    .saturating_sub(entry.buffered_bytes());
                let mut full = bytes::BytesMut::new();
                for idx in 0..entry.expected_chunks {
                    if let Some(chunk) = entry.chunk_payloads.remove(&idx) {
                        full.extend_from_slice(&chunk);
                    }
                }
                let assembled = full.freeze();
                // Pull the buffered Arcs out of the entry on the assembly
                // path. The final reassembled message owns the unique
                // `Arc`s (refcount = 1) — no clone needed, just a strip of
                // the chunk-only fields via `Arc::make_mut`.
                let first_chunk_message_id = entry.first_chunk_message_id;
                let redelivery_count = entry.redelivery_count;
                let mut final_meta_arc = entry.first_metadata;
                {
                    let m = std::sync::Arc::make_mut(&mut final_meta_arc);
                    m.num_chunks_from_msg = None;
                    m.chunk_id = None;
                    m.total_chunk_msg_size = None;
                }
                let bem = entry.broker_entry_metadata;

                // The "logical" message id is the *last* chunk's id (per Java
                // `ChunkMessageIdImpl.getLastChunkMessageId`). first_chunk_message_id is
                // already stored above; if the runtime needs it for ack, it should plumb it
                // via metadata properties.
                let _ = first_chunk_message_id;

                let im = IncomingMessage {
                    message_id,
                    metadata: final_meta_arc,
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
            // Wrap the per-batch metadata once so every sub-message shares
            // a refcount instead of deep-cloning. For a 100-message batch
            // this collapses 100 `MessageMetadata::clone()` calls (each of
            // which traverses every property, every encryption key, etc.)
            // into 100 Arc bumps.
            let shared_meta = std::sync::Arc::new(metadata);
            let shared_bem = broker_entry_metadata.map(std::sync::Arc::new);
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
                    metadata: shared_meta.clone(),
                    single_metadata: Some(single),
                    payload,
                    redelivery_count: redelivery,
                    broker_entry_metadata: shared_bem.clone(),
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
            metadata: std::sync::Arc::new(metadata),
            single_metadata: None,
            payload: body,
            redelivery_count: redelivery,
            broker_entry_metadata: broker_entry_metadata.map(std::sync::Arc::new),
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
        // Issue #349: `classify_and_queue` is called exactly once per
        // broker dispatch unit — once for a plain message, once per batch
        // member (the `deliver` batch loop), and once for the chunk-
        // completing logical message. Decrement the REAL balance
        // unconditionally, before the queued-vs-dead-lettered branch below:
        // the broker already spent one permit dispatching this entry
        // regardless of which branch the client routes it into. Issue #414:
        // the same call bumps the watchdog's progress mark — a dead-lettered
        // entry is still evidence the broker is dispatching to us.
        self.record_dispatch_unit();
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
            // Issue #301: track buffered-queue bytes for the `Auto` OOM guard.
            // Bumped here on enqueue, decremented in `pop_message` on dequeue.
            self.queued_bytes = self.queued_bytes.saturating_add(payload_len as u64);
            self.queue.push_back(msg);
            // `classify_and_queue` always appends exactly one message to the queue (the
            // batched-delivery loop in `deliver` calls this once per sub-message and
            // assembles its own `count`). The caller at `conn.rs` interprets `count`
            // as the number of *newly* delivered tail entries — returning
            // `self.queue.len()` would emit one `ConnectionEvent::Message` per queued
            // backlog entry on every arrival, scaling O(n²) with the receive queue.
            DeliverOutcome::Delivered { count: 1 }
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
        // Issue #348: a `next_active_change()` future already parked when the
        // user closes the consumer must resolve promptly instead of hanging
        // forever — mirror the receive-waker wake above exactly.
        self.wake_active_change_waiters();
    }

    /// Record a broker-reported Failover active/standby transition (issue
    /// #348). Sets [`Self::is_active`] to the new state, pushes it onto the
    /// bounded [`Self::active_changes`] ring (dropping the oldest entry once
    /// [`ACTIVE_CHANGES_CAP`] is reached), then drains and wakes every parked
    /// [`Self::active_change_wakers`] entry — the same drain-all fan-out
    /// semantic `wake_receivers` uses.
    pub fn record_active_change(&mut self, active: bool) {
        self.is_active = Some(active);
        if self.active_changes.len() >= ACTIVE_CHANGES_CAP {
            self.active_changes.pop_front();
        }
        self.active_changes.push_back(active);
        self.wake_active_change_waiters();
    }

    /// Pop the oldest not-yet-observed active-change transition, if any.
    /// Mirrors [`Self::pop_message`]'s FIFO-front semantics.
    pub fn pop_active_change(&mut self) -> Option<bool> {
        self.active_changes.pop_front()
    }

    /// Register a waker that fires when a new active-change transition is
    /// recorded or the consumer reaches a terminal state. Returns a slab key
    /// the caller MUST pass to [`Self::cancel_active_change_waker`] if the
    /// future is dropped before observing the wake. Mirrors
    /// [`Self::register_receive_waker`].
    pub fn register_active_change_waker(&mut self, waker: Waker) -> usize {
        self.active_change_wakers.insert(waker)
    }

    /// Evict a previously-registered active-change waker. Idempotent — a
    /// missing slot is a no-op (a concurrent wake may already have drained
    /// it). Mirrors [`Self::cancel_receive_waker`].
    pub fn cancel_active_change_waker(&mut self, slab_key: usize) {
        if self.active_change_wakers.contains(slab_key) {
            self.active_change_wakers.remove(slab_key);
        }
    }

    /// Drain every parked active-change waker and wake it. Mirrors
    /// [`Self::wake_receivers`] exactly — drain-all rather than wake-one, so
    /// every concurrent `next_active_change()` future re-polls and the first
    /// to acquire the connection lock pops the transition.
    fn wake_active_change_waiters(&mut self) {
        let wakers: Vec<Waker> = self.active_change_wakers.drain().collect();
        for w in wakers {
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
        assert_eq!(c.granted_permits, 100);
        assert_eq!(c.permit_balance, 100);
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
            let _ = c.pop_message(std::time::Instant::now());
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
        let msg = c.pop_message(std::time::Instant::now()).unwrap();
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
        let m1 = c.pop_message(std::time::Instant::now()).unwrap();
        let m2 = c.pop_message(std::time::Instant::now()).unwrap();
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
        let msg = c
            .pop_message(std::time::Instant::now())
            .expect("reassembled message");
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

    // -------------------------------------------------------------------
    // Issue #349 — `permit_balance` dispatch-unit accounting.
    //
    // The broker debits one permit per *message unit* dispatched: K for a
    // K-message batch entry, one per PIP-37 chunk, one per PIP-33 marker.
    // These four tests pin the exact decrement count per shape.
    // -------------------------------------------------------------------

    #[test]
    fn permit_balance_decrements_per_dispatch_unit_normal() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        assert_eq!(c.permit_balance, 100);
        c.deliver(
            &message_cmd(0),
            metadata(1),
            None,
            Bytes::from_static(b"x"),
            std::time::Instant::now(),
        )
        .unwrap();
        assert_eq!(
            c.permit_balance, 99,
            "one plain single message consumes exactly one dispatch unit"
        );
    }

    #[test]
    fn permit_balance_decrements_per_dispatch_unit_batch() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        assert_eq!(c.permit_balance, 100);

        // Build a batch payload: two singles with their length-prefixed metadata
        // (mirrors `batch_message_explodes`).
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
        assert_eq!(
            c.permit_balance, 98,
            "a K=2 batch entry consumes exactly K=2 dispatch units"
        );
    }

    #[test]
    fn permit_balance_decrements_per_dispatch_unit_chunk() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        assert_eq!(c.permit_balance, 100);

        let make_chunk = |idx: i32, payload: &'static [u8]| {
            let mut meta = pb::MessageMetadata {
                producer_name: "p".to_owned(),
                sequence_id: 1,
                publish_time: 1_700_000_000,
                ..Default::default()
            };
            meta.num_chunks_from_msg = Some(3);
            meta.chunk_id = Some(idx);
            meta.uuid = Some("u-shape".to_owned());
            meta.total_chunk_msg_size = Some(6);
            (meta, Bytes::from_static(payload))
        };

        let (m0, p0) = make_chunk(0, b"aa");
        c.deliver(&message_cmd(0), m0, None, p0, std::time::Instant::now())
            .unwrap();
        assert_eq!(
            c.permit_balance, 99,
            "the first (incomplete) chunk consumes one dispatch unit"
        );

        let (m1, p1) = make_chunk(1, b"bb");
        c.deliver(&message_cmd(0), m1, None, p1, std::time::Instant::now())
            .unwrap();
        assert_eq!(
            c.permit_balance, 98,
            "the second (incomplete) chunk consumes one dispatch unit"
        );

        let (m2, p2) = make_chunk(2, b"cc");
        let outcome = c
            .deliver(&message_cmd(0), m2, None, p2, std::time::Instant::now())
            .unwrap();
        assert!(matches!(outcome, DeliverOutcome::Delivered { .. }));
        assert_eq!(
            c.permit_balance, 97,
            "the completing (3rd) chunk consumes the final dispatch unit — an \
             N=3-chunk message decrements exactly N=3 total"
        );
    }

    #[test]
    fn permit_balance_decrements_per_dispatch_unit_marker() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        assert_eq!(c.permit_balance, 100);
        c.record_marker_consumed();
        assert_eq!(
            c.permit_balance, 99,
            "one PIP-33 marker consumes exactly one dispatch unit"
        );
    }

    #[test]
    fn permit_balance_decrements_for_dlq_routed_message_too() {
        // The dead-letter branch of `classify_and_queue` is still one broker
        // dispatch unit — the broker already spent the permit dispatching
        // the entry, regardless of whether the client routes it to the user
        // queue or diverts it to the DLQ pending list on this arrival.
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        c.max_redeliver_count = 2;
        let _ = c.initial_flow();
        assert_eq!(c.permit_balance, 100);
        c.deliver(
            &message_cmd(5),
            metadata(1),
            None,
            Bytes::from_static(b"poison"),
            std::time::Instant::now(),
        )
        .unwrap();
        assert_eq!(
            c.dead_letter_pending.len(),
            1,
            "routed to DLQ, not the queue"
        );
        assert_eq!(
            c.permit_balance, 99,
            "a DLQ-routed message still consumes exactly one dispatch unit"
        );
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
        let hist = c
            .receive_latency_hist
            .as_mut()
            .expect("receive_latency_hist initialised");
        for v in 1u64..=100 {
            hist.saturating_record(v);
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

    /// Scripted receive dwell for the latency tests below. Kept `<= 2047`: at 3 significant
    /// figures `hdrhistogram` only round-trips values below the 2048 sub-bucket boundary
    /// exactly, so a larger constant would compare against a quantised neighbour.
    const RECEIVE_DWELL_MS: u64 = 250;

    /// Deterministic receive-latency stamping (ADR-0086): `pop_message` records exactly
    /// `now - msg.arrived_at`, both instants injected, with no host-clock read. Every value
    /// here is derived from one synthetic base, so the recorded sample is an exact constant
    /// on every run and on every host.
    #[test]
    fn pop_message_records_receive_latency() {
        let base = std::time::Instant::now();
        let arrived = base + std::time::Duration::from_millis(100);
        let popped = arrived + std::time::Duration::from_millis(RECEIVE_DWELL_MS);

        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        c.deliver(
            &message_cmd(0),
            metadata(1),
            None,
            Bytes::from_static(b"x"),
            arrived,
        )
        .unwrap();
        assert!(
            c.receive_latency_hist
                .as_ref()
                .is_none_or(hdrhistogram::Histogram::is_empty)
        );

        let _msg = c.pop_message(popped).expect("queued message");
        assert_eq!(
            c.receive_latency_hist
                .as_ref()
                .map_or(0, hdrhistogram::Histogram::len),
            1
        );
        assert_eq!(
            c.receive_latency_max_ms(),
            RECEIVE_DWELL_MS,
            "the sample must be the injected `now - arrived_at`, not a host-clock elapsed"
        );
        let stats = c.stats();
        assert_eq!(stats.receive_latency_max_ms, c.receive_latency_max_ms());
    }

    /// The leak assertion proper (ADR-0086): host-clock motion between delivery and pop must
    /// not move the recorded sample. Before the fix, `pop_message` read
    /// `msg.arrived_at.elapsed()`, so the `thread::sleep` below landed straight in the
    /// histogram and the two runs disagreed.
    #[test]
    fn pop_message_latency_is_immune_to_host_clock_motion() {
        let arrived = std::time::Instant::now();
        let popped = arrived + std::time::Duration::from_millis(RECEIVE_DWELL_MS);

        let sample = |host_sleep: std::time::Duration| -> u64 {
            let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
            let _ = c.initial_flow();
            c.deliver(
                &message_cmd(0),
                metadata(1),
                None,
                Bytes::from_static(b"x"),
                arrived,
            )
            .unwrap();
            // The host clock genuinely moves here; the injected instants do not.
            std::thread::sleep(host_sleep);
            let _ = c.pop_message(popped).expect("queued message");
            c.receive_latency_max_ms()
        };

        let without_sleep = sample(std::time::Duration::ZERO);
        let with_sleep = sample(std::time::Duration::from_millis(120));
        assert_eq!(
            without_sleep, with_sleep,
            "the recorded latency moved with the HOST clock — the Instant::elapsed() leak is back"
        );
        assert_eq!(without_sleep, RECEIVE_DWELL_MS);
    }

    /// Invariant #6: a `now` that precedes `arrived_at` (a caller reusing a stale snapshot, or
    /// a virtual clock rewound across a reset) records 0 — it must NOT panic. The naive fix
    /// `now - msg.arrived_at` uses the `Sub` impl, which panics on underflow; this test is the
    /// guard against that.
    #[test]
    fn pop_message_records_zero_when_now_precedes_arrival() {
        let base = std::time::Instant::now();
        // Arrival stamped "in the future" relative to the pop instant.
        let arrived = base + std::time::Duration::from_mins(1);

        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        c.deliver(
            &message_cmd(0),
            metadata(1),
            None,
            Bytes::from_static(b"x"),
            arrived,
        )
        .unwrap();

        let _msg = c.pop_message(base).expect("queued message");
        assert_eq!(
            c.receive_latency_hist
                .as_ref()
                .map_or(0, hdrhistogram::Histogram::len),
            1
        );
        assert_eq!(
            c.receive_latency_max_ms(),
            0,
            "a clock regression must clamp to 0, never saturate to u64::MAX"
        );
    }

    // ---------------------------------------------------------------------
    // Issue #347: aggregate_stats zeroes fields. `ConsumerState::
    // receive_latency_histogram` exposes the raw distribution so
    // `ConsumerStats::fold` can merge several consumers' histograms into a
    // statistically sound aggregate percentile (percentiles don't compose
    // under per-child summing or maxing — only a real histogram merge is
    // sound).
    // ---------------------------------------------------------------------

    /// [`ConsumerState::receive_latency_histogram`] hands back a clone of the
    /// live `receive_latency_hist` — independent of further mutation on the
    /// source consumer — for callers (the engine `ConsumerApi::
    /// receive_latency_histogram` accessor, `ConsumerStats::fold`) that need
    /// the raw distribution rather than the three pre-computed percentiles
    /// `stats()` exposes.
    #[test]
    fn receive_latency_histogram_accessor_returns_clone_of_recorded_samples() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        assert!(
            c.receive_latency_histogram().is_none_or(|h| h.is_empty()),
            "no samples recorded yet"
        );

        let hist = c
            .receive_latency_hist
            .as_mut()
            .expect("receive_latency_hist initialised");
        for v in [10u64, 20, 30] {
            hist.saturating_record(v);
        }

        let snapshot = c
            .receive_latency_histogram()
            .expect("histogram present after recording");
        assert_eq!(snapshot.len(), 3);
        assert_eq!(snapshot.value_at_quantile(1.0), 30);

        // Mutate the live histogram after the snapshot was taken — the clone
        // must not retroactively observe it (proves it's a real clone, not a
        // shared reference / view).
        c.receive_latency_hist
            .as_mut()
            .expect("still initialised")
            .saturating_record(999);
        assert_eq!(
            snapshot.len(),
            3,
            "snapshot must be independent of later recordings on the source consumer"
        );
    }

    /// `ConsumerStats::fold` propagates every field per its documented rule:
    /// the six cumulative totals + `pending_batch_acks` sum (saturating);
    /// `msgs_per_sec` / `bytes_per_sec` sum as f64; `receive_latency_max_ms`
    /// is the exact max; `receive_latency_p50_ms` / `receive_latency_p99_ms`
    /// are recomputed from the REAL merged histogram, not from either
    /// child's own (here deliberately stale) percentile fields.
    ///
    /// Two children with a clean 60/40 sample split at two widely-separated
    /// latencies (10ms / 500ms) make the merged percentiles unambiguous:
    /// the merged p50 falls within the 60-sample low cluster (10ms) and the
    /// merged p99 falls within the 40-sample high cluster (500ms) — neither
    /// matches either child's stale field, and neither is a sum/max of the
    /// children's own percentile fields (which would be 555/666 or 444).
    #[test]
    fn consumer_stats_fold_propagates_every_field() {
        let mut hist1 = hdrhistogram::Histogram::<u64>::new(3).expect("histogram");
        for _ in 0..60 {
            hist1.saturating_record(10);
        }
        let mut hist2 = hdrhistogram::Histogram::<u64>::new(3).expect("histogram");
        for _ in 0..40 {
            hist2.saturating_record(500);
        }

        let s1 = ConsumerStats {
            total_msgs_received: 100,
            total_bytes_received: 1_000,
            total_acks_sent: 90,
            total_acks_failed: 1,
            total_msgs_dead_lettered: 2,
            total_chunked_msgs_received: 3,
            // Deliberately stale/wrong — fold must ignore these and recompute
            // from the merged histogram instead.
            receive_latency_p50_ms: 111,
            receive_latency_p99_ms: 222,
            receive_latency_max_ms: 10,
            msgs_per_sec: 5.5,
            bytes_per_sec: 55.5,
            pending_batch_acks: 4,
        };
        let s2 = ConsumerStats {
            total_msgs_received: 20,
            total_bytes_received: 200,
            total_acks_sent: 15,
            total_acks_failed: 0,
            total_msgs_dead_lettered: 0,
            total_chunked_msgs_received: 1,
            receive_latency_p50_ms: 333,
            receive_latency_p99_ms: 444,
            receive_latency_max_ms: 500,
            msgs_per_sec: 1.5,
            bytes_per_sec: 15.0,
            pending_batch_acks: 6,
        };

        let folded = ConsumerStats::fold([(s1, Some(hist1)), (s2, Some(hist2))]);

        assert_eq!(folded.total_msgs_received, 120);
        assert_eq!(folded.total_bytes_received, 1_200);
        assert_eq!(folded.total_acks_sent, 105);
        assert_eq!(folded.total_acks_failed, 1);
        assert_eq!(folded.total_msgs_dead_lettered, 2);
        assert_eq!(folded.total_chunked_msgs_received, 4);
        assert_eq!(
            folded.receive_latency_max_ms, 500,
            "max is the exact max across children, never summed"
        );
        assert!((folded.msgs_per_sec - 7.0).abs() < f64::EPSILON);
        assert!((folded.bytes_per_sec - 70.5).abs() < f64::EPSILON);
        assert_eq!(folded.pending_batch_acks, 10);

        assert_eq!(
            folded.receive_latency_p50_ms, 10,
            "merged p50 must land in the 60-sample low cluster, not either \
             child's stale field"
        );
        assert_eq!(
            folded.receive_latency_p99_ms, 500,
            "merged p99 must land in the 40-sample high cluster (real \
             histogram merge), not a sum/max of the children's own stale \
             percentile fields"
        );
    }

    /// When no child supplies a histogram, `fold` still sums the totals and
    /// takes the exact max of `receive_latency_max_ms` (a plain stats-field
    /// rule, independent of the histogram), but the percentiles — which can
    /// ONLY come from a real histogram merge — read zero.
    #[test]
    fn consumer_stats_fold_with_no_histograms_yields_zero_percentiles() {
        let s1 = ConsumerStats {
            total_msgs_received: 5,
            receive_latency_max_ms: 15,
            ..ConsumerStats::default()
        };
        let s2 = ConsumerStats {
            total_msgs_received: 7,
            receive_latency_max_ms: 25,
            ..ConsumerStats::default()
        };

        let folded = ConsumerStats::fold([(s1, None), (s2, None)]);

        assert_eq!(folded.total_msgs_received, 12);
        assert_eq!(
            folded.receive_latency_max_ms, 25,
            "max is a stats-field rule, unaffected by histogram absence"
        );
        assert_eq!(folded.receive_latency_p50_ms, 0);
        assert_eq!(folded.receive_latency_p99_ms, 0);
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

        let msg = c
            .pop_message(std::time::Instant::now())
            .expect("immediate delivery");
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
        let msg = c
            .pop_message(std::time::Instant::now())
            .expect("reassembled message");
        assert_eq!(msg.payload.as_ref(), b"aaabbbcccc");
        assert_eq!(c.stats().total_chunked_msgs_received, 1);
        // Reassembled message must not carry chunk markers downstream.
        assert!(msg.metadata.chunk_id.is_none());
        assert!(msg.metadata.num_chunks_from_msg.is_none());
        assert!(msg.metadata.total_chunk_msg_size.is_none());
    }

    /// Out-of-order chunk arrival AFTER the first chunk established the buffer
    /// (chunk 2 before chunk 1) must still reassemble into the correct payload
    /// because the buffer is keyed by `chunk_id`. Although the broker normally
    /// dispatches chunks in order, reconnection races and replay can interleave
    /// them — the buffer logic is defensive.
    ///
    /// The bounded-reassembly hardening gates BUFFER CREATION on a genuine
    /// first chunk (`chunk_id == 0`), so chunk 0 must arrive first to open the
    /// buffer; the remaining chunks may then interleave. A non-first chunk for
    /// an unknown uuid is dropped (see
    /// [`straggler_non_first_chunk_for_unknown_uuid_is_dropped`]).
    #[test]
    fn out_of_order_chunks_are_buffered_correctly() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();

        // Chunk 0 first (opens the buffer), then chunk 2, then chunk 1.
        let order: [(i32, &[u8]); 3] = [(0, b"AAAA"), (2, b"ZZZZ"), (1, b"BBBB")];
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
        let msg = c
            .pop_message(std::time::Instant::now())
            .expect("reassembled");
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
        let a = c.pop_message(std::time::Instant::now()).expect("A");
        let b = c.pop_message(std::time::Instant::now()).expect("B");
        assert_eq!(a.payload.as_ref(), b"A0A1");
        assert_eq!(b.payload.as_ref(), b"B0B1");
    }

    // ---------------------------------------------------------------------
    // Bounded chunk reassembly (DoS hardening). A hostile/buggy broker that
    // streams distinct-UUID first chunks that never complete used to grow
    // `chunk_reassembly` without bound (OOM). These tests pin the Java-matching
    // breadth cap (`max_pending_chunked_message`), eviction policy
    // (`auto_ack_oldest_chunked_message_on_queue_full`), the expiry sweep
    // (`expire_time_of_incomplete_chunked_message`, wired through BOTH
    // `poll_timeout` and `handle_timeout`), and the depth-axis `total` bound.
    // Mirror Java `ConsumerImpl#removeOldestPendingChunkedMessage` /
    // `removeExpireIncompleteChunkedMessages`. Each asserts a bound that does
    // NOT hold on main, proving the bug.
    // ---------------------------------------------------------------------

    /// Deliver a single never-completing FIRST chunk (`chunk_id == 0`) of a
    /// `total`-chunk message identified by `uuid`. Returns the outcome.
    fn deliver_first_chunk(
        c: &mut ConsumerState,
        uuid: &str,
        seq: u64,
        total: i32,
        now: std::time::Instant,
    ) -> DeliverOutcome {
        let meta = chunk_meta(uuid, seq, total, 0);
        c.deliver(
            &message_cmd_at(seq, 0),
            meta,
            None,
            Bytes::from_static(b"first-chunk-body"),
            now,
        )
        .unwrap()
    }

    /// At the cap of 10, an 11th distinct never-completing UUID must EVICT the
    /// oldest buffer rather than grow the map past the cap. On main the map is
    /// unbounded, so this fails (`pending_chunk_count()` would be 11).
    #[test]
    fn eleventh_incomplete_uuid_evicts_oldest_at_cap_ten() {
        let now = std::time::Instant::now();
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        assert_eq!(
            c.max_pending_chunked_message, 10,
            "Java-matching default cap"
        );

        for i in 0..10 {
            let outcome = deliver_first_chunk(&mut c, &format!("u-{i}"), i as u64, 3, now);
            assert!(matches!(outcome, DeliverOutcome::Buffered));
        }
        assert_eq!(c.pending_chunk_count(), 10, "exactly the cap is buffered");

        // The 11th distinct never-completing UUID evicts the oldest (u-0).
        let outcome = deliver_first_chunk(&mut c, "u-10", 10, 3, now);
        assert!(matches!(outcome, DeliverOutcome::Buffered));
        assert_eq!(
            c.pending_chunk_count(),
            10,
            "map must stay bounded at the cap, not grow to 11"
        );
        assert!(
            !c.chunk_reassembly.contains_key("u-0"),
            "the oldest UUID must be the one evicted"
        );
        assert!(
            c.chunk_reassembly.contains_key("u-10"),
            "newest is retained"
        );
    }

    /// Eviction with `auto_ack = false` (the default) DROPS the partial without
    /// acking — `chunk_auto_ack_pending` stays empty so the broker redelivers.
    #[test]
    fn eviction_auto_ack_false_does_not_ack_partial() {
        let now = std::time::Instant::now();
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        c.max_pending_chunked_message = 2;
        assert!(!c.auto_ack_oldest_chunked_message_on_queue_full);

        deliver_first_chunk(&mut c, "u-0", 0, 3, now);
        deliver_first_chunk(&mut c, "u-1", 1, 3, now);
        deliver_first_chunk(&mut c, "u-2", 2, 3, now); // evicts u-0

        assert_eq!(c.pending_chunk_count(), 2);
        assert!(!c.chunk_reassembly.contains_key("u-0"));
        assert!(
            c.chunk_auto_ack_pending.is_empty(),
            "auto_ack=false must NOT stage an ack for the evicted partial"
        );
    }

    /// Eviction with `auto_ack = true` ACKS the evicted partial's first-chunk
    /// id (staged into `chunk_auto_ack_pending`) then drops it.
    #[test]
    fn eviction_auto_ack_true_acks_partial() {
        let now = std::time::Instant::now();
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        c.max_pending_chunked_message = 2;
        c.auto_ack_oldest_chunked_message_on_queue_full = true;

        // seq 0's first chunk carries entry_id 0 → that is the staged ack id.
        deliver_first_chunk(&mut c, "u-0", 0, 3, now);
        deliver_first_chunk(&mut c, "u-1", 1, 3, now);
        deliver_first_chunk(&mut c, "u-2", 2, 3, now); // evicts u-0, acks it

        assert_eq!(c.pending_chunk_count(), 2);
        assert!(!c.chunk_reassembly.contains_key("u-0"));
        assert_eq!(
            c.chunk_auto_ack_pending.len(),
            1,
            "auto_ack=true must stage exactly one ack for the evicted partial"
        );
        assert_eq!(c.chunk_auto_ack_pending[0].entry_id, 0);
    }

    /// A straggler non-first chunk (`chunk_id = 2`) for an unknown/evicted UUID
    /// must be DROPPED and must NOT fabricate a fresh buffer from non-first
    /// metadata. On main the `or_insert_with` would create a corrupt buffer.
    #[test]
    fn straggler_non_first_chunk_for_unknown_uuid_is_dropped() {
        let now = std::time::Instant::now();
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();

        let meta = chunk_meta("u-orphan", 7, 3, 2); // chunk_id 2, never saw 0/1
        let outcome = c
            .deliver(
                &message_cmd_at(7, 0),
                meta,
                None,
                Bytes::from_static(b"orphan"),
                now,
            )
            .unwrap();
        assert!(
            matches!(outcome, DeliverOutcome::Dropped),
            "a straggler non-first chunk must be Dropped, got {outcome:?}"
        );
        assert_eq!(
            c.pending_chunk_count(),
            0,
            "no fresh buffer may be created from non-first metadata"
        );
    }

    /// The expiry sweep removes a buffer older than
    /// `expire_time_of_incomplete_chunked_message`, AND `next_chunk_expiry_deadline()`
    /// returns its deadline so `poll_timeout` can schedule the wake. Asserted
    /// through BOTH accessors (the AUDIT-CRITICAL wiring). On main there is no
    /// expiry field, no sweep, and no deadline — this fails.
    #[test]
    fn expiry_sweep_removes_stale_buffer_and_surfaces_deadline() {
        let t0 = std::time::Instant::now();
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        assert_eq!(
            c.expire_time_of_incomplete_chunked_message,
            Some(std::time::Duration::from_mins(1)),
            "Java-matching 60s default"
        );

        deliver_first_chunk(&mut c, "u-stale", 0, 3, t0);
        assert_eq!(c.pending_chunk_count(), 1);

        // The deadline poll_timeout would surface is t0 + 60s.
        let deadline = c
            .next_chunk_expiry_deadline()
            .expect("an incomplete buffer must expose an expiry deadline");
        assert_eq!(deadline, t0 + std::time::Duration::from_mins(1));

        // A sweep before the deadline is a no-op.
        c.sweep_expired_chunks(t0 + std::time::Duration::from_secs(59));
        assert_eq!(c.pending_chunk_count(), 1, "not yet expired");

        // A sweep past the deadline removes the stale buffer.
        c.sweep_expired_chunks(t0 + std::time::Duration::from_secs(61));
        assert_eq!(c.pending_chunk_count(), 0, "stale buffer must be swept");
        assert!(
            c.next_chunk_expiry_deadline().is_none(),
            "no deadline once the map is empty"
        );
    }

    /// Depth axis: a chunk advertising an absurd `total` (> MAX_CHUNK_TOTAL)
    /// must be rejected before it can pre-size the reassembly structures. On
    /// main `total` is never bounded — this fails (the chunk would buffer).
    #[test]
    fn absurd_total_chunk_count_is_rejected() {
        let now = std::time::Instant::now();
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();

        let meta = chunk_meta("u-huge", 1, i32::MAX, 0);
        let outcome = c
            .deliver(
                &message_cmd_at(1, 0),
                meta,
                None,
                Bytes::from_static(b"x"),
                now,
            )
            .unwrap();
        assert!(
            matches!(outcome, DeliverOutcome::Dropped),
            "an absurd total chunk count must be Dropped, got {outcome:?}"
        );
        assert_eq!(c.pending_chunk_count(), 0, "no buffer for an absurd total");

        // A total exactly at the cap is admitted (boundary check).
        let meta_ok = chunk_meta("u-okay", 2, MAX_CHUNK_TOTAL, 0);
        let outcome_ok = c
            .deliver(
                &message_cmd_at(2, 0),
                meta_ok,
                None,
                Bytes::from_static(b"x"),
                now,
            )
            .unwrap();
        assert!(matches!(outcome_ok, DeliverOutcome::Buffered));
        assert_eq!(c.pending_chunk_count(), 1);
    }

    /// Public-API regression guard (the same shape proven to FAIL on main).
    /// Deliver 11 distinct never-completing first chunks of 2-chunk messages,
    /// then complete the OLDEST (u-0). The cap-10 + eviction fix evicted u-0, so
    /// its second chunk is a straggler for an unknown uuid → Dropped → the queue
    /// stays empty. On unbounded main u-0's buffer survives → completes → the
    /// queue would hold one message.
    #[test]
    fn oldest_incomplete_is_evicted_at_cap_ten_public_api() {
        let now = std::time::Instant::now();
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();

        for i in 0..11u64 {
            let meta = chunk_meta(&format!("u-{i}"), i, 2, 0);
            let _ = c
                .deliver(
                    &message_cmd_at(i, 0),
                    meta,
                    None,
                    Bytes::from_static(b"c0"),
                    now,
                )
                .unwrap();
        }

        let meta = chunk_meta("u-0", 0, 2, 1);
        let _ = c
            .deliver(
                &message_cmd_at(100, 0),
                meta,
                None,
                Bytes::from_static(b"c1"),
                now,
            )
            .unwrap();

        assert_eq!(
            c.queue_len(),
            0,
            "the oldest incomplete message must have been evicted at the cap; \
             on unbounded main it survives and completes (queue == 1)"
        );
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
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        // chunkMsgId2 := (first=2/2/2, last=3/3/3) — its "logical" id is 3/3/3.
        let id2 = MessageId {
            ledger_id: 3,
            entry_id: 3,
            partition: 3,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
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
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let logical_id_of_chunk2 = MessageId {
            ledger_id: 3,
            entry_id: 3,
            partition: 3,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };

        // A plain `MessageId` matching the lastChunkMessageId of chunk1.
        let plain = MessageId {
            ledger_id: 1,
            entry_id: 1,
            partition: 1,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
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
        let msg = c
            .pop_message(std::time::Instant::now())
            .expect("reassembled message");
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
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            },
            metadata: std::sync::Arc::new(meta),
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

    // -----------------------------------------------------------------
    // Invariant-#6 (no panics in `magnetar-proto` outside `#[cfg(test)]`)
    // and F1 (chunk_id range check) regression tests.
    // -----------------------------------------------------------------

    /// V10: `ConsumerState::new` must not panic via `.expect(...)` on the
    /// statically-valid `hdrhistogram::Histogram::new(3)`. Smoke-test that the
    /// constructor returns `Some(_)` and that the `receive_latency_*_ms`
    /// accessors degrade gracefully (return 0) when the histogram is empty.
    #[test]
    fn consumer_latency_histogram_constructs_without_panic() {
        let c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        assert!(
            c.receive_latency_hist.is_some(),
            "Histogram::new(3) is statically valid",
        );
        assert_eq!(c.receive_latency_p50_ms(), 0);
        assert_eq!(c.receive_latency_p99_ms(), 0);
        assert_eq!(c.receive_latency_max_ms(), 0);
        let stats = c.stats();
        assert_eq!(stats.receive_latency_p50_ms, 0);
        assert_eq!(stats.receive_latency_p99_ms, 0);
        assert_eq!(stats.receive_latency_max_ms, 0);
    }

    /// V11: chunk reassembly used `.remove(&uuid).expect("just-inserted
    /// ChunkBuffer disappeared")`. The fix replaces it with an `if let`
    /// guard that returns `DeliverOutcome::Dropped` if the buffer is
    /// somehow absent — graceful, not a panic. Verify the normal path
    /// (all chunks present) still reassembles correctly so the fix did
    /// not regress the happy path.
    #[test]
    fn chunk_reassembly_remove_happy_path_still_delivers() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        // Three in-order chunks of a logical message.
        for chunk_id in 0..3 {
            let meta = chunk_meta("u-happy", 1, 3, chunk_id);
            let outcome = c
                .deliver(
                    &message_cmd_at(100 + chunk_id as u64, 0),
                    meta,
                    None,
                    Bytes::from_static(b"chunk"),
                    std::time::Instant::now(),
                )
                .unwrap();
            let _ = outcome;
        }
        // The reassembled message is now in the queue and the buffer is gone.
        assert_eq!(c.queue_len(), 1);
        assert!(c.chunk_reassembly.is_empty());
    }

    /// F1: chunks whose `chunk_id` falls outside `[0, total_chunks)` are
    /// protocol violations. Feed `chunk_id = i32::MAX, total_chunks = 3` and
    /// `chunk_id = -1, total_chunks = 3` and assert each is gracefully
    /// dropped — never reaches the reassembly buffer, never panics,
    /// never advances the chunk counter. Mirrors the defensive
    /// bounds-check Java added in `ConsumerImpl#processMessageChunk`.
    #[test]
    fn chunk_id_out_of_range_drops_protocol_violation() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        // chunk_id > total - 1
        let meta_high = chunk_meta("u-bad-high", 1, 3, i32::MAX);
        let outcome_high = c
            .deliver(
                &message_cmd_at(1, 0),
                meta_high,
                None,
                Bytes::from_static(b"hi"),
                std::time::Instant::now(),
            )
            .expect("delivery must not error");
        assert!(
            matches!(outcome_high, DeliverOutcome::Dropped),
            "chunk_id == i32::MAX with total == 3 must be Dropped, got {outcome_high:?}",
        );
        // chunk_id < 0
        let meta_neg = chunk_meta("u-bad-neg", 1, 3, -1);
        let outcome_neg = c
            .deliver(
                &message_cmd_at(2, 0),
                meta_neg,
                None,
                Bytes::from_static(b"hi"),
                std::time::Instant::now(),
            )
            .expect("delivery must not error");
        assert!(
            matches!(outcome_neg, DeliverOutcome::Dropped),
            "chunk_id == -1 must be Dropped, got {outcome_neg:?}",
        );
        // No reassembly buffer was ever allocated and no message was queued.
        assert!(
            c.chunk_reassembly.is_empty(),
            "out-of-range chunk_id must not allocate a ChunkBuffer entry",
        );
        assert_eq!(c.queue_len(), 0);
    }

    /// F1 corollary: a boundary case — `chunk_id == total_chunks` is also
    /// out-of-range (chunks are zero-indexed in `[0, total)`). The Java
    /// defensive bounds-check rejects this exact off-by-one too.
    #[test]
    fn chunk_id_equals_total_chunks_is_dropped() {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        // 3 chunks ⇒ valid ids are {0, 1, 2}. id == 3 is OOB.
        let meta = chunk_meta("u-boundary", 1, 3, 3);
        let outcome = c
            .deliver(
                &message_cmd_at(1, 0),
                meta,
                None,
                Bytes::from_static(b"oob"),
                std::time::Instant::now(),
            )
            .expect("delivery must not error");
        assert!(
            matches!(outcome, DeliverOutcome::Dropped),
            "chunk_id == total_chunks must be Dropped, got {outcome:?}",
        );
        assert!(c.chunk_reassembly.is_empty());
    }
}

// ---------------------------------------------------------------------------
// Issue #414 — per-consumer stall watchdog state machine.
//
// The failure this models: a Pulsar Shared subscription whose broker-side
// dispatcher wedges after consumer churn. The survivors hold the permits the
// broker acknowledged, their local queues are empty, and nothing ever arrives
// again — while the connection keepalive keeps passing, so ADR-0058's
// connection watchdog never fires.
//
// The whole machine lives on `ConsumerState`: a monotonic dispatch counter as
// the progress signal, and one `StallWatch` opened / advanced / discarded by
// `poll_stall`. These tests pin every arm of it.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod stall_watchdog_tests {
    use bytes::Bytes;

    use super::*;

    const WINDOW: std::time::Duration = std::time::Duration::from_secs(30);

    fn metadata() -> pb::MessageMetadata {
        pb::MessageMetadata {
            producer_name: "p".to_owned(),
            sequence_id: 1,
            publish_time: 1_700_000_000,
            num_messages_in_batch: Some(1),
            ..Default::default()
        }
    }

    fn message_cmd() -> pb::CommandMessage {
        pb::CommandMessage {
            consumer_id: 1,
            message_id: pb::MessageIdData {
                ledger_id: 1,
                entry_id: 1,
                ..Default::default()
            },
            redelivery_count: Some(0),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }
    }

    /// A granted consumer with an empty queue — precisely the shape issue #414
    /// wedges in.
    fn granted() -> ConsumerState {
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        let _ = c.initial_flow();
        c
    }

    fn deliver_one(c: &mut ConsumerState, at: std::time::Instant) {
        c.deliver(
            &message_cmd(),
            metadata(),
            None,
            Bytes::from_static(b"x"),
            at,
        )
        .expect("deliver");
    }

    #[test]
    fn granted_consumer_with_empty_queue_is_a_stall_candidate() {
        assert!(
            granted().is_stall_candidate(),
            "un-spent permits over an empty queue is exactly the #414 shape"
        );
    }

    #[test]
    fn fresh_consumer_is_not_a_candidate_before_any_grant() {
        let c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        assert!(
            !c.is_stall_candidate(),
            "no grant means the broker was never asked to dispatch — silence is correct"
        );
    }

    #[test]
    fn queued_messages_suppress_candidacy() {
        let t0 = std::time::Instant::now();
        let mut c = granted();
        deliver_one(&mut c, t0);
        assert!(
            !c.is_stall_candidate(),
            "a non-empty queue means the user, not the broker, owes the progress"
        );
    }

    /// Every state that has its own explanation for the silence must suppress
    /// the watchdog — otherwise it reports the caller's own gating back at them.
    #[test]
    fn user_and_protocol_gating_suppress_candidacy() {
        for (label, mutate) in [
            (
                "closed",
                (|c: &mut ConsumerState| c.closed = true) as fn(&mut ConsumerState),
            ),
            ("paused", |c: &mut ConsumerState| c.paused = true),
            ("reached_end_of_topic", |c: &mut ConsumerState| {
                c.reached_end_of_topic = true;
            }),
            ("pending_seek", |c: &mut ConsumerState| {
                c.pending_seek = Some(crate::types::RequestId(7));
            }),
            ("terminal_failure", |c: &mut ConsumerState| {
                c.terminal_failure = Some("dead".to_owned());
            }),
            ("flow_on_subscribe_ack", |c: &mut ConsumerState| {
                c.flow_on_subscribe_ack = true;
            }),
        ] {
            let mut c = granted();
            assert!(c.is_stall_candidate(), "{label}: candidate before mutation");
            mutate(&mut c);
            assert!(
                !c.is_stall_candidate(),
                "{label} must suppress the stall watchdog"
            );
        }
    }

    #[test]
    fn first_tick_only_seeds_the_window_and_arms_the_deadline() {
        let t0 = std::time::Instant::now();
        let mut c = granted();
        assert_eq!(
            c.next_stall_deadline(WINDOW),
            None,
            "no window yet: there is no instant to arm a deadline from"
        );
        assert_eq!(
            c.poll_stall(WINDOW, t0),
            None,
            "the seeding tick never reports — the window starts here"
        );
        assert_eq!(
            c.next_stall_deadline(WINDOW),
            Some(t0 + WINDOW),
            "poll_timeout must now be able to schedule the deterministic wake"
        );
    }

    #[test]
    fn stall_fires_once_at_the_window_and_never_again_in_the_same_episode() {
        let t0 = std::time::Instant::now();
        let mut c = granted();
        assert_eq!(c.poll_stall(WINDOW, t0), None, "seed");
        assert_eq!(
            c.poll_stall(
                WINDOW,
                t0 + WINDOW.saturating_sub(std::time::Duration::from_millis(1))
            ),
            None,
            "a tick one millisecond short of the window must not fire"
        );
        assert_eq!(
            c.poll_stall(WINDOW, t0 + WINDOW),
            Some(WINDOW),
            "the tick at the deadline reports the exact silence duration"
        );
        for extra in [1u64, 5, 60] {
            assert_eq!(
                c.poll_stall(WINDOW, t0 + WINDOW + std::time::Duration::from_secs(extra)),
                None,
                "one event per stall episode, however many ticks run past the deadline"
            );
        }
        assert_eq!(
            c.next_stall_deadline(WINDOW),
            None,
            "a reported window arms no further wake"
        );
    }

    #[test]
    fn a_late_tick_reports_the_real_silence_not_the_configured_window() {
        let t0 = std::time::Instant::now();
        let mut c = granted();
        assert_eq!(c.poll_stall(WINDOW, t0), None, "seed");
        let late = std::time::Duration::from_secs(95);
        assert_eq!(
            c.poll_stall(WINDOW, t0 + late),
            Some(late),
            "the event carries how long the silence actually lasted"
        );
    }

    #[test]
    fn a_dispatch_restarts_the_window_and_re_arms_the_episode_latch() {
        let t0 = std::time::Instant::now();
        let mut c = granted();
        assert_eq!(c.poll_stall(WINDOW, t0), None, "seed");
        assert_eq!(
            c.poll_stall(WINDOW, t0 + WINDOW),
            Some(WINDOW),
            "first stall"
        );

        // Recovery: one message arrives and the user drains it.
        let recovered_at = t0 + WINDOW + std::time::Duration::from_secs(1);
        deliver_one(&mut c, recovered_at);
        let _ = c.pop_message(recovered_at);
        assert!(
            c.is_stall_candidate(),
            "back to un-spent permits over an empty queue"
        );

        // The dispatch moved the progress mark, so the next tick opens a FRESH
        // window rather than re-reporting the old one.
        assert_eq!(c.poll_stall(WINDOW, recovered_at), None, "re-seed");
        assert_eq!(
            c.poll_stall(
                WINDOW,
                recovered_at + WINDOW.saturating_sub(std::time::Duration::from_millis(1))
            ),
            None,
            "the new window is measured from the dispatch, not from the first stall"
        );
        assert_eq!(
            c.poll_stall(WINDOW, recovered_at + WINDOW),
            Some(WINDOW),
            "a consumer that wedges twice reports twice"
        );
    }

    /// The regression this guards: a consumer draining a long backlog holds a
    /// queue for minutes, so candidacy is false throughout; the instant it
    /// empties and `maybe_flow` re-grants, a stale window would fire a stall
    /// immediately even though the broker has done nothing wrong.
    #[test]
    fn losing_and_regaining_candidacy_restarts_the_window_from_scratch() {
        let t0 = std::time::Instant::now();
        let mut c = granted();
        assert_eq!(c.poll_stall(WINDOW, t0), None, "seed");

        // A message arrives: candidacy drops while it sits in the queue.
        let arrived = t0 + std::time::Duration::from_secs(5);
        deliver_one(&mut c, arrived);
        assert_eq!(
            c.poll_stall(WINDOW, arrived),
            None,
            "not a candidate — the window is discarded"
        );
        assert_eq!(
            c.next_stall_deadline(WINDOW),
            None,
            "and no deadline is armed while the user owes the progress"
        );

        // The user drains it much later than the window.
        let drained = arrived + std::time::Duration::from_secs(600);
        let _ = c.pop_message(drained);
        assert_eq!(
            c.poll_stall(WINDOW, drained),
            None,
            "regaining candidacy only re-seeds — it must NOT inherit the pre-queue window"
        );
        assert_eq!(
            c.poll_stall(
                WINDOW,
                drained + WINDOW.saturating_sub(std::time::Duration::from_millis(1))
            ),
            None,
            "the new window runs from the drain, so a stall is still one millisecond away"
        );
    }

    #[test]
    fn a_fresh_grant_restarts_the_window() {
        let t0 = std::time::Instant::now();
        let mut c = granted();
        assert_eq!(c.poll_stall(WINDOW, t0), None, "seed");
        // A re-attach re-arms the full grant: the broker has a brand-new promise
        // to honour and deserves a brand-new window.
        let rearmed = t0 + std::time::Duration::from_secs(29);
        let _ = c.initial_flow();
        assert_eq!(
            c.next_stall_deadline(WINDOW),
            None,
            "initial_flow drops the open window"
        );
        assert_eq!(c.poll_stall(WINDOW, rearmed), None, "re-seed at the re-arm");
        assert_eq!(
            c.poll_stall(WINDOW, rearmed + WINDOW),
            Some(WINDOW),
            "and the new window is measured from the re-arm"
        );
    }

    /// ADR-0103: the automatic-recovery budget is restored by a broker dispatch unit and
    /// by nothing else.
    ///
    /// The two negative halves matter more than the positive one. `initial_flow` and
    /// `clear_stall_watch` are exactly what an automatic recovery attempt performs on its
    /// way out, so a reset hanging off either would refund every attempt that bought it
    /// and the bound would not exist at all; `record_marker_consumed` is the PIP-33
    /// filtered-marker path, which IS a spent broker permit and therefore IS progress.
    #[test]
    fn only_a_dispatch_unit_restores_the_auto_recovery_budget() {
        let t0 = std::time::Instant::now();

        // Neither half of a recovery attempt refunds the budget.
        let mut recovering = granted();
        recovering.stall_recovery_attempts = 3;
        recovering.clear_stall_watch();
        assert_eq!(
            recovering.stall_recovery_attempts, 3,
            "dropping the stall window is not progress"
        );
        let _ = recovering.initial_flow();
        assert_eq!(
            recovering.stall_recovery_attempts, 3,
            "a fresh grant is a fresh promise, not a kept one: the broker acking a \
             re-subscribe must not refund the attempt that paid for it"
        );

        // A dispatch unit does.
        let mut dispatched = granted();
        dispatched.stall_recovery_attempts = 3;
        deliver_one(&mut dispatched, t0);
        assert_eq!(
            dispatched.stall_recovery_attempts, 0,
            "one message actually arriving is the definition of progress"
        );

        // Including a PIP-33 marker the user never sees — the broker spent a permit on it.
        let mut marked = granted();
        marked.stall_recovery_attempts = 2;
        marked.record_marker_consumed();
        assert_eq!(marked.stall_recovery_attempts, 0);
    }

    /// Every dispatch shape the broker can spend a permit on must count as
    /// progress. A shape that decremented `permit_balance` without bumping the
    /// mark would make the watchdog cry stall on a healthy consumer.
    #[test]
    fn every_dispatch_unit_shape_bumps_the_progress_mark() {
        let t0 = std::time::Instant::now();

        // Plain message.
        let mut plain = granted();
        deliver_one(&mut plain, t0);
        assert_eq!(plain.dispatch_units_received, 1);

        // Batch member: one mark per exploded member.
        let mut batched = granted();
        let mut batch_meta = metadata();
        batch_meta.num_messages_in_batch = Some(3);
        let mut body = bytes::BytesMut::new();
        for i in 0..3u8 {
            let single = pb::SingleMessageMetadata {
                payload_size: 1,
                ..Default::default()
            };
            let encoded = single.encode_to_vec();
            body.extend_from_slice(&(u32::try_from(encoded.len()).expect("fits")).to_be_bytes());
            body.extend_from_slice(&encoded);
            body.extend_from_slice(&[i]);
        }
        batched
            .deliver(&message_cmd(), batch_meta, None, body.freeze(), t0)
            .expect("deliver batch");
        assert_eq!(
            batched.dispatch_units_received, 3,
            "each batch member is one broker dispatch unit"
        );

        // PIP-33 marker: filtered off the user stream but still a spent permit.
        let mut marked = granted();
        marked.record_marker_consumed();
        assert_eq!(marked.dispatch_units_received, 1);
    }

    #[test]
    fn an_incomplete_chunk_counts_as_progress() {
        let t0 = std::time::Instant::now();
        let mut c = granted();
        let chunk_meta = pb::MessageMetadata {
            producer_name: "p".to_owned(),
            sequence_id: 1,
            publish_time: 1_700_000_000,
            uuid: Some("chunked-progress".to_owned()),
            num_chunks_from_msg: Some(2),
            chunk_id: Some(0),
            total_chunk_msg_size: Some(2),
            ..Default::default()
        };
        let outcome = c
            .deliver(
                &message_cmd(),
                chunk_meta,
                None,
                Bytes::from_static(b"a"),
                t0,
            )
            .expect("buffer chunk");
        assert!(matches!(outcome, DeliverOutcome::Buffered));
        assert_eq!(
            c.dispatch_units_received, 1,
            "a consumer mid-reassembly is receiving; the broker is not silent"
        );
        assert!(
            !c.is_stall_candidate() || c.queue.is_empty(),
            "candidacy is unaffected by reassembly state; only progress matters"
        );
        assert_eq!(
            c.poll_stall(WINDOW, t0),
            None,
            "the seeding tick latches the post-chunk mark"
        );
    }

    #[test]
    fn a_disabled_or_zero_permit_consumer_never_arms_a_deadline() {
        let t0 = std::time::Instant::now();
        // Zero balance: the broker owes us nothing.
        let mut c = ConsumerState::new(ConsumerHandle(1), "t".to_owned(), "s".to_owned(), 100);
        assert_eq!(c.poll_stall(WINDOW, t0), None);
        assert_eq!(c.next_stall_deadline(WINDOW), None);
        // And a candidate whose window is dropped by `clear_stall_watch` arms
        // nothing until the next tick re-seeds it.
        let mut granted = granted();
        assert_eq!(granted.poll_stall(WINDOW, t0), None);
        granted.clear_stall_watch();
        assert_eq!(granted.next_stall_deadline(WINDOW), None);
    }
}
